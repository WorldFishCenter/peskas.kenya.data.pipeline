from pymongo import MongoClient, errors
from pymongo.collection import ReturnDocument


class MongoDBManager:
    def __init__(self, dbname, col_name=None):
        conn_string = "mongodb+srv://peskasplatform:GUMRcKLWCz08jUzM@peskasdb.4slojhc.mongodb.net/?retryWrites=true&w=majority&appName=PeskasDB"
        self.client = MongoClient(conn_string)
        self.db = self.client[dbname]
        self.collection = self.db[col_name] if col_name is not None else None

    def set_table_name(self, col_name):
        self.collection = self.db[col_name]

    def insert_document(self, document):
        """Inserts a single document into the collection."""
        if self.collection is None:
            print('Please specify the Collection name first..')
            return
        try:
            result = self.collection.insert_one(document)
            return result.inserted_id
        except errors.PyMongoError as e:
            print(f"An error occurred: {e}")
            return None

    def insert_multiple_documents(self, documents, batch_size=100):
        """Inserts multiple documents into the collection."""
        if self.collection is None:
            print('Please specify the Collection name first..')
            return
        cnt = 0
        print(len(documents))
        try:
            for i in range(0, len(documents), batch_size):
                cnt+=1
                print('inserting batch ', cnt)
                batch = documents[i:i + batch_size]
                self.collection.insert_many(batch)
            # return True
        except errors.PyMongoError as e:
            print(f"An error occurred: {e}")
            # return False

    def find_document(self, query, projection=None):
        """Finds a single document based on a query and optional projection."""
        if self.collection is None:
            print('Please specify the Collection name first..')
            return

        try:
            return self.collection.find_one(query, projection)
        except errors.PyMongoError as e:
            print(f"An error occurred: {e}")
            return None

    def find_documents(self, query, projection=None):
        """Finds documents based on a query and optional projection."""
        if self.collection is None:
            print('Please specify the Collection name first..')
            return
        try:
            return list(self.collection.find(query, projection))
        except errors.PyMongoError as e:
            print(f"An error occurred: {e}")
            return []

    def update_document(self, query, update, upsert=False):
        """Updates a single document based on a query."""
        if self.collection is None:
            print('Please specify the Collection name first..')
            return
        try:
            result = self.collection.find_one_and_update(
                query, update, upsert=upsert, return_document=ReturnDocument.AFTER)
            return result
        except errors.PyMongoError as e:
            print(f"An error occurred: {e}")
            return None

    def delete_document(self, query):
        """Deletes a single document based on a query."""
        if self.collection is None:
            print('Please specify the Collection name first..')
            return
        try:
            result = self.collection.delete_one(query)
            return result.deleted_count
        except errors.PyMongoError as e:
            print(f"An error occurred: {e}")
            return 0

    def close_connection(self):
        """Closes the MongoDB connection."""
        self.client.close()
