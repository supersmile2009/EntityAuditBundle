<?php
/*
 * (c) 2011 SimpleThings GmbH
 *
 * @package SimpleThings\EntityAudit
 * @author Benjamin Eberlei <eberlei@simplethings.de>
 * @link http://www.simplethings.de
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

namespace SimpleThings\EntityAudit\EventListener;

use Doctrine\Common\EventSubscriber;
use Doctrine\DBAL\Types\Type;
use Doctrine\ORM\Event\LifecycleEventArgs;
use Doctrine\ORM\Event\OnFlushEventArgs;
use Doctrine\ORM\Event\PostFlushEventArgs;
use Doctrine\ORM\Events;
use Doctrine\ORM\Mapping\ClassMetadata;
use Doctrine\ORM\Persisters\Entity\BasicEntityPersister;
use Doctrine\ORM\Persisters\Entity\EntityPersister;
use Doctrine\ORM\UnitOfWork;
use SimpleThings\EntityAudit\AuditManager;

class LogRevisionsListener implements EventSubscriber
{
    /**
     * @var \SimpleThings\EntityAudit\AuditConfiguration
     */
    protected $config;

    /**
     * @var \SimpleThings\EntityAudit\Metadata\MetadataFactory
     */
    protected $metadataFactory;

    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $conn;

    /**
     * @var \Doctrine\DBAL\Platforms\AbstractPlatform
     */
    protected $platform;

    /**
     * @var \Doctrine\ORM\EntityManager
     */
    protected $em;

    /**
     * @var \Doctrine\ORM\Mapping\QuoteStrategy
     */
    protected $quoteStrategy;

    /**
     * @var array
     */
    protected $insertRevisionSQL = array();

    /**
     * @var \Doctrine\ORM\UnitOfWork
     */
    protected $uow;

    /**
     * @var int
     */
    protected $revisionId;

    /**
     * @var array
     */
    protected $extraUpdates = array();

    /**
     * @var array
     */
    protected $entityDeletions = array();

    /**
     * @var array
     */
    protected $entityInserts = array();

    /**
     * @var array
     */
    protected $entityUpdates = array();

    public function __construct(AuditManager $auditManager)
    {
        $this->config = $auditManager->getConfiguration();
        $this->metadataFactory = $auditManager->getMetadataFactory();
    }

    public function getSubscribedEvents()
    {
        return array(Events::onFlush, Events::postPersist, Events::postUpdate, Events::postFlush);
    }

    /**
     * @param PostFlushEventArgs $postFlushEventArgs
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function postFlush(PostFlushEventArgs $postFlushEventArgs)
    {
        foreach ($this->entityInserts as $entity) {
            $class = $this->em->getClassMetadata(\get_class($entity));
            $this->saveRevisionEntityData($class, $this->getOriginalEntityData($entity), 'INS');
        }

        foreach ($this->entityUpdates as $entity) {
            // get changes => should be already computed here (is a listener)
            $changeset = $this->uow->getEntityChangeSet($entity);
            foreach ($this->config->getGlobalIgnoreColumns() as $column) {
                if (isset($changeset[$column])) {
                    unset($changeset[$column]);
                }
            }

            // if we have no changes left => don't create revision log
            if (empty($changeset)) {
                return;
            }

            $class = $this->em->getClassMetadata(\get_class($entity));
            // handle the case when identifier is also an association.
            // getEntityIdentifier() returns just association id, not the whole entity.
            $identifier = $this->uow->getEntityIdentifier($entity);
            foreach ($identifier as $propertyName => $value) {
                if (isset($class->associationMappings[$propertyName])) {
                    $associationMetadata = $this->em->getClassMetadata($class->associationMappings[$propertyName]['targetEntity']);
                    $identifier[$propertyName] = $this->uow->tryGetById(
                        $value,
                        $associationMetadata->rootEntityName
                    );
                }
            }

            $entityData = array_merge($this->getOriginalEntityData($entity), $identifier);
            $this->saveRevisionEntityData($class, $entityData, 'UPD');
        }
        foreach ($this->entityDeletions as $entityDeletion) {
            list($entity, $identifier) = $entityDeletion;
            $class = $this->em->getClassMetadata(\get_class($entity));
            // If entity remains managed after delete, it means we're dealing with SoftDeleteable.
            // Recompute changeset to fetch fresh "deleted" state for a revi
            if ($this->uow->getEntityState($entity, false) === UnitOfWork::STATE_MANAGED) {
                $this->uow->recomputeSingleEntityChangeSet($class, $entity);
            }
            $entityData = array_merge($this->getOriginalEntityData($entity), $identifier);
            $this->saveRevisionEntityData($class, $entityData, 'DEL');
        }


        $this->entityDeletions =
        $this->entityInserts =
        $this->entityUpdates = [];
    }

    /**
     * @param LifecycleEventArgs $eventArgs
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function postPersist(LifecycleEventArgs $eventArgs)
    {
        // onFlush was executed before, everything already initialized
        $entity = $eventArgs->getEntity();

        $class = $this->em->getClassMetadata(\get_class($entity));
        if (! $this->metadataFactory->isAudited($class->name)) {
            return;
        }

        $this->entityInserts[] = $entity;
    }

    /**
     * @param LifecycleEventArgs $eventArgs
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function postUpdate(LifecycleEventArgs $eventArgs)
    {
        // onFlush was executed before, everything already initialized
        $entity = $eventArgs->getEntity();

        $class = $this->em->getClassMetadata(\get_class($entity));
        if (! $this->metadataFactory->isAudited($class->name)) {
            return;
        }

        $this->entityUpdates[] = $entity;
    }

    /**
     * @param OnFlushEventArgs $eventArgs
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function onFlush(OnFlushEventArgs $eventArgs)
    {
        $this->em = $eventArgs->getEntityManager();
        $this->quoteStrategy = $this->em->getConfiguration()->getQuoteStrategy();
        $this->conn = $this->em->getConnection();
        $this->uow = $this->em->getUnitOfWork();
        $this->platform = $this->conn->getDatabasePlatform();
        $this->revisionId = null; // reset revision

        $processedEntities = array();

        foreach ($this->uow->getScheduledEntityDeletions() AS $entity) {
            //doctrine is fine deleting elements multiple times. We are not.
            $hash = $this->getHash($entity);

            if (\in_array($hash, $processedEntities, true)) {
                continue;
            }

            $processedEntities[] = $hash;

            $class = $this->em->getClassMetadata(\get_class($entity));
            if (! $this->metadataFactory->isAudited($class->name)) {
                continue;
            }
            $this->entityDeletions[] = [$entity, $this->uow->getEntityIdentifier($entity)];
        }
    }

    /**
     * get original entity data, including versioned field, if "version" constraint is used
     *
     * @param mixed $entity
     *
     * @return array
     */
    protected function getOriginalEntityData($entity)
    {
        $class = $this->em->getClassMetadata(\get_class($entity));
        $data = $this->uow->getOriginalEntityData($entity);
        if ($class->isVersioned) {
            $versionField = $class->versionField;
            $data[$versionField] = $class->reflFields[$versionField]->getValue($entity);
        }

        return $data;
    }

    /**
     * @return int|string
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function getRevisionId()
    {
        if ($this->revisionId === null) {
            $this->conn->insert(
                $this->config->getRevisionTableName(),
                array(
                    'timestamp' => date_create('now'),
                    'username' => $this->config->getCurrentUsername(),
                ),
                array(
                    Type::DATETIME,
                    Type::STRING,
                )
            );

            $sequenceName = $this->platform->supportsSequences()
                ? $this->platform->getIdentitySequenceName($this->config->getRevisionTableName(), 'id')
                : null;

            $this->revisionId = $this->conn->lastInsertId($sequenceName);
        }

        return $this->revisionId;
    }

    /**
     * @param ClassMetadata $class
     *
     * @return string
     * @throws \Doctrine\DBAL\DBALException
     */
    private function getInsertRevisionSQL($class)
    {
        if (! isset($this->insertRevisionSQL[$class->name])) {
            $placeholders = array('?', '?');
            $tableName = $this->config->getTableName($class);

            $sql = 'INSERT INTO '. $tableName .' ('.
                $this->config->getRevisionFieldName() .', '. $this->config->getRevisionTypeFieldName();

            $fields = array();

            foreach ($class->associationMappings as $field => $assoc) {
                if ($class->isInheritanceTypeJoined() && $class->isInheritedAssociation($field)) {
                    continue;
                }

                if (($assoc['type'] & ClassMetadata::TO_ONE) > 0 && $assoc['isOwningSide']) {
                    foreach ($assoc['targetToSourceKeyColumns'] as $sourceCol) {
                        $fields[$sourceCol] = true;
                        $sql .= ', ' . $sourceCol;
                        $placeholders[] = '?';
                    }
                }
            }

            foreach ($class->fieldNames as $field) {
                if (array_key_exists($field, $fields)) {
                    continue;
                }

                if ($class->isInheritanceTypeJoined()
                    && $class->isInheritedField($field)
                    && ! $class->isIdentifier($field)
                ) {
                    continue;
                }

                $type = Type::getType($class->fieldMappings[$field]['type']);
                $placeholders[] = ! empty($class->fieldMappings[$field]['requireSQLConversion'])
                    ? $type->convertToDatabaseValueSQL('?', $this->platform)
                    : '?';
                $sql .= ', ' . $this->quoteStrategy->getColumnName($field, $class, $this->platform);
            }

            if (($class->isInheritanceTypeJoined() && $class->rootEntityName === $class->name)
                || $class->isInheritanceTypeSingleTable()
            ) {
                $sql .= ', ' . $class->discriminatorColumn['name'];
                $placeholders[] = '?';
            }

            $sql .= ') VALUES ('. implode(', ', $placeholders) .')';
            $this->insertRevisionSQL[$class->name] = $sql;
        }

        return $this->insertRevisionSQL[$class->name];
    }

    /**
     * @param ClassMetadata $class
     * @param array $entityData
     * @param string $revType
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function saveRevisionEntityData($class, $entityData, $revType)
    {
        $params = array($this->getRevisionId(), $revType);
        $types = array(\PDO::PARAM_INT, \PDO::PARAM_STR);

        $fields = array();

        foreach ($class->associationMappings AS $field => $assoc) {
            if ($class->isInheritanceTypeJoined() && $class->isInheritedAssociation($field)) {
                continue;
            }
            if (! (($assoc['type'] & ClassMetadata::TO_ONE) > 0 && $assoc['isOwningSide'])) {
                continue;
            }

            $data = isset($entityData[$field]) ? $entityData[$field] : null;
            $relatedId = false;

            if ($data !== null && $this->uow->isInIdentityMap($data)) {
                $relatedId = $this->uow->getEntityIdentifier($data);
            }

            $targetClass = $this->em->getClassMetadata($assoc['targetEntity']);

            foreach ($assoc['sourceToTargetKeyColumns'] as $sourceColumn => $targetColumn) {
                $fields[$sourceColumn] = true;
                if ($data === null) {
                    $params[] = null;
                    $types[] = \PDO::PARAM_STR;
                } else {
                    $params[] = $relatedId ? $relatedId[$targetClass->fieldNames[$targetColumn]] : null;
                    $types[] = $targetClass->getTypeOfColumn($targetColumn);
                }
            }
        }

        foreach ($class->fieldNames AS $field) {
            if (array_key_exists($field, $fields)) {
                continue;
            }

            if ($class->isInheritanceTypeJoined()
                && $class->isInheritedField($field)
                && ! $class->isIdentifier($field)
            ) {
                continue;
            }

            $params[] = isset($entityData[$field]) ? $entityData[$field] : null;
            $types[] = $class->fieldMappings[$field]['type'];
        }

        if ($class->isInheritanceTypeSingleTable()) {
            $params[] = $class->discriminatorValue;
            $types[] = $class->discriminatorColumn['type'];
        } elseif ($class->isInheritanceTypeJoined()) {
            if ($class->name === $class->rootEntityName) {
                $params[] = $entityData[$class->discriminatorColumn['name']];
                $types[] = $class->discriminatorColumn['type'];
            } else {
                $entityData[$class->discriminatorColumn['name']] = $class->discriminatorValue;
                $this->saveRevisionEntityData(
                    $this->em->getClassMetadata($class->rootEntityName),
                    $entityData,
                    $revType
                );
            }
        }

        $this->conn->executeUpdate($this->getInsertRevisionSQL($class), $params, $types);
    }

    /**
     * @param $entity
     *
     * @return string
     */
    protected function getHash($entity)
    {
        return implode(
            ' ',
            array_merge(
                array(\get_class($entity)),
                $this->uow->getEntityIdentifier($entity)
            )
        );
    }

    /**
     * Modified version of BasicEntityPersister::prepareUpdateData()
     * git revision d9fc5388f1aa1751a0e148e76b4569bd207338e9 (v2.5.3)
     *
     * @license MIT
     *
     * @author  Roman Borschel <roman@code-factory.org>
     * @author  Giorgio Sironi <piccoloprincipeazzurro@gmail.com>
     * @author  Benjamin Eberlei <kontakt@beberlei.de>
     * @author  Alexander <iam.asm89@gmail.com>
     * @author  Fabio B. Silva <fabio.bat.silva@gmail.com>
     * @author  Rob Caiger <rob@clocal.co.uk>
     * @author  Simon Mönch <simonmoench@gmail.com>
     *
     * @param EntityPersister|BasicEntityPersister $persister
     * @param                 $entity
     *
     * @return array
     *
     * @throws \Doctrine\ORM\Mapping\MappingException
     */
    protected function prepareUpdateData($persister, $entity)
    {
        $uow = $this->em->getUnitOfWork();
        $classMetadata = $persister->getClassMetadata();

        $versionField = null;
        $result = array();

        if ($classMetadata->isVersioned === true) {
            $versionField = $classMetadata->versionField;
        }

        foreach ($uow->getEntityChangeSet($entity) as $field => $change) {
            if ($versionField !== null && $versionField === $field) {
                continue;
            }

            if (isset($classMetadata->embeddedClasses[$field])) {
                continue;
            }

            $newVal = $change[1];

            if ( ! isset($classMetadata->associationMappings[$field])) {
                $columnName = $classMetadata->columnNames[$field];
                $result[$persister->getOwningTable($field)][$columnName] = $newVal;

                continue;
            }

            $assoc = $classMetadata->associationMappings[$field];

            // Only owning side of x-1 associations can have a FK column.
            if ( ! $assoc['isOwningSide'] || ! ($assoc['type'] & ClassMetadata::TO_ONE)) {
                continue;
            }

            if ($newVal !== null) {
                if ($uow->isScheduledForInsert($newVal)) {
                    $newVal = null;
                }
            }

            $newValId = null;

            if ($newVal !== null) {
                if (! $uow->isInIdentityMap($newVal)) {
                    continue;
                }

                $newValId = $uow->getEntityIdentifier($newVal);
            }

            $targetClass = $this->em->getClassMetadata($assoc['targetEntity']);
            $owningTable = $persister->getOwningTable($field);

            foreach ($assoc['joinColumns'] as $joinColumn) {
                $sourceColumn = $joinColumn['name'];
                $targetColumn = $joinColumn['referencedColumnName'];

                $result[$owningTable][$sourceColumn] = $newValId
                    ? $newValId[$targetClass->getFieldForColumn($targetColumn)]
                    : null;
            }
        }

        return $result;
    }
}
