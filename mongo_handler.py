from datetime import datetime, timedelta
from pymongo.mongo_client import MongoClient


class MongoHandler:

    @staticmethod
    def combine_bonds_sectors_ratios_insiders_options_in_order(bonds, sectors, ratios, insiders, options):
        result = []
        result.extend(bonds)
        result.extend(sectors)
        result.extend(ratios)
        result.extend(insiders)
        result.extend(options)
        return result

    @staticmethod
    def create_nested_dict(name, timestamp, additional_info):
        return {
            'name': name,
            'timestamp': timestamp,
            'additional_info': additional_info
        }

    @staticmethod
    def package_data_for_mongo(list_of_dict):
        collection = []
        current_time = datetime.now()
        row_names = ["week-bonds", "month-bonds", "quarter-bonds", "week-sectors", "month-sectors",
                     "quarter-sectors", "week-ratios", "month-ratios", "quarter-ratios",
                     "week-insiders", "month-insiders", "quarter-insiders", "week-options", "month-options",
                     "quarter-options", "meta-week-options", "meta-month-options", "meta-quarter-options"]
        i = 0
        for dictionary in list_of_dict:
            if i==16:
                print("16th iteration")
                print(dictionary)
                print(row_names[16])


            new_dict = MongoHandler.create_nested_dict(row_names[i], current_time, dictionary)
            collection.append(new_dict)
            i += 1;
        return collection

    @staticmethod
    def delete_documents_older_than(mongo_collection, days):
        # Calculate the timestamp days ago
        days_ago = datetime.utcnow() - timedelta(days=days)

        # Delete documents with a timestamp earlier than days_ago
        delete_result = mongo_collection.delete_many({'timestamp': {'$lt': days_ago}})

        # Print the number of documents that were deleted
        print(f"{delete_result.deleted_count} documents deleted")

    @staticmethod
    def save_to_mongo_collection(documents, db_name, collection_name):
        uri = MongoHandler.get_mongodb_uri()
        client = MongoClient(uri)
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
        db = client[db_name]
        mongo_collection = db[collection_name]
        #MongoHandler.delete_documents_older_than(mongo_collection, 5)
        result = mongo_collection.insert_many(documents)
        client.close()
        return result


    @staticmethod
    def get_mongodb_uri_from_file(file_path):
        try:
            with open(file_path, 'r') as file:
                mongodb_uri = file.read().strip()
                if not mongodb_uri:
                    raise ValueError("The file is empty or contains only whitespace.")
                return mongodb_uri
        except FileNotFoundError:
            raise FileNotFoundError(f"The file at {file_path} was not found.")
        except Exception as e:
            raise RuntimeError(f"An error occurred while reading the file: {str(e)}")

    @staticmethod
    def get_mongodb_uri():
        uri = MongoHandler.get_mongodb_uri_from_file("mongoUri.txt")
        return uri

    @staticmethod
    def get_mongodb_db(file_path):
        try:
            with open(file_path, 'r') as file:
                mongodb_db = file.read().strip()
                if not mongodb_db:
                    raise ValueError("The file is empty or contains only whitespace.")
                return mongodb_db
        except FileNotFoundError:
            raise FileNotFoundError(f"The file at {file_path} was not found.")
        except Exception as e:
            raise RuntimeError(f"An error occurred while reading the file: {str(e)}")


if __name__ == "__main__":
    uri = MongoHandler.get_mongodb_uri()
    client = MongoClient(uri)
    mongo_db_name = MongoHandler.get_mongodb_db("mongoDB.txt")
    db = client[mongo_db_name]
    mongo_collection = db["basic_data"]
    MongoHandler.delete_documents_older_than(mongo_collection, 5)
    print("complete")
