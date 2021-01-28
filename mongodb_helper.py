import logging

from pymongo import MongoClient, UpdateOne


class MongodbHelper:
    def __init__(self, host='192.168.31.22', port=27017, db='sanguo'):
        self._host = host
        self._port = port
        self._client = MongoClient(host=host, port=port)
        self._db = self._client.get_database(db)

    def __del__(self):
        pass

    def do_bulk_upsert(self,
                       col: str,
                       data: list,
                       filter_key: list,
                       set_key: list = None,
                       set_on_insert_key: list = None):
        try:
            collection = self._db[col]
            requests = list()
            for d in data:
                filter_ = dict()
                for key in filter_key:
                    filter_.update({key: d.get(key)})
                set_on_insert = dict()
                set_ = dict()
                if set_on_insert_key:
                    for key in set_on_insert_key:
                        value = d.get(key)
                        set_on_insert[key] = value
                if set_key:
                    for key in set_key:
                        value = d.get(key)
                        set_[key] = value
                if set_key:
                    update = {'$set': set_,
                              '$setOnInsert': set_on_insert}
                else:
                    update = {'$setOnInsert': set_on_insert}
                request = UpdateOne(filter=filter_, update=update, upsert=True)
                requests.append(request)
            if requests:
                result = collection.bulk_write(requests=requests, ordered=True)
                logging.info(f'DoBulkWrite, '
                             f'col({col}), total({len(data)}), '
                             f'modified({result.modified_count}), upserted({result.upserted_count})')
                return result
            else:
                logging.info(f'DoBulkWrite, requests is empty, filter key({filter_key})')
                return None
        except Exception as ex:
            logging.exception(ex)
            return None


