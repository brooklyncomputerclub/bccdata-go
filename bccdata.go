package bccdata

import (
	"database/sql"
	"fmt"
	"time"
)

type DatabaseContext struct {
	Database           *sql.DB
	EntityDescriptions map[string]EntityDescription
}

type EntityRelationship struct {
	EntityName    string
	JoinTableName string
	ForeignKey    string
	TargetKey     string
}

type EntityDescription struct {
	Name               string
	TableName          string
	PrimaryKey         string
	Relationships      map[string]EntityRelationship
	InsertStatement    *sql.Stmt
	CreateZeroInstance func() Entity
	Context            *DatabaseContext
}

type Entity interface {
	ScanFromRow(*sql.Rows) (bool, error)
}

// Entity Descriptions

func (databaseContext *DatabaseContext) RegisterEntityDescription(entityDescription EntityDescription) {
	if databaseContext.EntityDescriptions == nil {
		databaseContext.EntityDescriptions = make(map[string]EntityDescription)
	}

	entityDescription.Context = databaseContext

	databaseContext.EntityDescriptions[entityDescription.Name] = entityDescription
}

func (databaseContext *DatabaseContext) EntityDescriptionForName(entityName string) (entityDescription EntityDescription) {
	return databaseContext.EntityDescriptions[entityName]
}

// Entity Relationships

func (entityDescription *EntityDescription) RegisterRelationship(entityRelationship EntityRelationship) {
	if entityDescription.Relationships == nil {
		entityDescription.Relationships = make(map[string]EntityRelationship)
	}

	entityDescription.Relationships[entityRelationship.EntityName] = entityRelationship
}

func (entityDescription *EntityDescription) RelationshipForName(entityName string) (entityRelationship EntityRelationship) {
	return entityDescription.Relationships[entityName]
}

// Entity Creation

func (entityDescription *EntityDescription) Create(transaction *sql.Tx, args ...interface{}) (entity Entity, err error) {
	var (
		commitAtEnd          bool
		insertStatement      *sql.Stmt
		result               sql.Result
		objectID             int64
		createdTime          int64
		tableName            string
		updateCreatedDateSQL string
		querySQL             string
		rows                 *sql.Rows
		scanSuccess          bool
	)

	commitAtEnd = false
	if transaction == nil {
		transaction, err = entityDescription.Context.Database.Begin()
		commitAtEnd = true
		if err != nil {
			goto cleanup
		}

		commitAtEnd = true
	}

	insertStatement = transaction.Stmt(entityDescription.InsertStatement)

	result, err = insertStatement.Exec(args...)
	if err != nil {
		goto cleanup
	}

	objectID, err = result.LastInsertId()
	if err != nil {
		goto cleanup
	}

	tableName = entityDescription.TableName

	createdTime = time.Now().Unix()
	updateCreatedDateSQL = fmt.Sprintf("UPDATE %s SET createdDate=? WHERE id=?", tableName)
	result, err = transaction.Exec(updateCreatedDateSQL, createdTime, objectID)
	if err != nil {
		goto cleanup
	}

	querySQL = fmt.Sprintf("SELECT * FROM %s WHERE id=?", tableName)
	rows, err = transaction.Query(querySQL, objectID)
	if err != nil {
		goto cleanup
	}

	entity = entityDescription.CreateZeroInstance()
	scanSuccess, err = entity.ScanFromRow(rows)
	if !scanSuccess {
		goto cleanup
	}

cleanup:
	if rows != nil {
		defer rows.Close()
	}

	if insertStatement != nil {
		defer insertStatement.Close()
	}

	if commitAtEnd {
		if err != nil {
			transaction.Rollback()
		} else {
			transaction.Commit()
		}
	}

	return entity, err
}

func (entityDescription *EntityDescription) CreateFromRows(rows *sql.Rows) (entities []Entity, err error) {
	var (
		scanSuccess bool
	)

	for {
		entity := entityDescription.CreateZeroInstance()

		scanSuccess, err = entity.ScanFromRow(rows)
		if !scanSuccess {
			break
		}

		entities = append(entities, entity)
	}

	return entities, err
}

// Entity Find

func (entityDescription *EntityDescription) FindEntity(transaction *sql.Tx, keyName *string, value interface{}) (entity Entity, err error) {
	entities, err := entityDescription.FindEntities(transaction, keyName, value)
	return entities[0], err
}

func (entityDescription *EntityDescription) FindEntities(transaction *sql.Tx, keyName *string, value interface{}) (entities []Entity, err error) {
	var (
		tableName       string
		columnName      string
		selectStatement string
		rows            *sql.Rows
	)

	tableName = entityDescription.TableName

	if keyName == nil {
		columnName = entityDescription.PrimaryKey
	} else {
		columnName = *keyName
	}

	selectStatement = fmt.Sprintf("SELECT * FROM %s WHERE %s=?", tableName, columnName)

	if transaction != nil {
		rows, err = transaction.Query(selectStatement, value)
	} else {
		rows, err = entityDescription.Context.Database.Query(selectStatement, value)
	}

	if err != nil {
		goto cleanup
	}

	entities, err = entityDescription.CreateFromRows(rows)

	if err != nil {
		goto cleanup
	}

cleanup:
	defer rows.Close()

	return entities, err
}

func (entityDescription *EntityDescription) FindRelatedEntity(transaction *sql.Tx, targetEntityName string, queryKey string, queryValue interface{}) (entities []Entity, err error) {
	var (
		relationship            EntityRelationship
		targetEntityDescription EntityDescription
		joinTableName           string
		targetTableName         string
		joinTableForeignKey     string
		targetTableKey          string
		selectStatement         string
		rows                    *sql.Rows
	)

	relationship = entityDescription.RelationshipForName(targetEntityName)
	targetEntityDescription = entityDescription.Context.EntityDescriptionForName(targetEntityName)

	joinTableName = relationship.JoinTableName
	targetTableName = targetEntityDescription.TableName

	joinTableForeignKey = relationship.ForeignKey
	targetTableKey = relationship.TargetKey

	// SELECT * FROM lists_placemarks LEFT OUTER JOIN placemarks ON lists_placemarks.placemarksID=placemarks.id WHERE lists_placemarks.listsID=1
	selectStatement = fmt.Sprintf("SELECT %s.* FROM %s LEFT OUTER JOIN %s ON %s.%s=%s.%s WHERE %s.%s=?", targetTableName, joinTableName, targetTableName, joinTableName, joinTableForeignKey, targetTableName, targetTableKey, joinTableName, queryKey)

	if transaction != nil {
		rows, err = transaction.Query(selectStatement, queryValue)
	} else {
		rows, err = entityDescription.Context.Database.Query(selectStatement, queryValue)
	}

	entities, err = targetEntityDescription.CreateFromRows(rows)

	return entities, err
}
