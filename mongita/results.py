class InsertOneResult():
    def __init__(self, inserted_id):
        self.acknowledged = True
        self._inserted_id = inserted_id

    @property
    def inserted_id(self):
        return self._inserted_id


class InsertManyResult():
    def __init__(self, documents):
        self.acknowledged = True
        self._inserted_ids = [doc['_id'] for doc in documents]

    @property
    def inserted_ids(self):
        return self._inserted_ids


class DeleteResult():
    def __init__(self, deleted_count):
        self.acknowledged = True
        self._deleted_count = deleted_count

    @property
    def deleted_count(self):
        return self._deleted_count

    @property
    def raw_result(self):
        return {'n': self._deleted_count, 'ok': 1.0}


class UpdateResult():
    def __init__(self, matched_count, modified_count, upserted_id=None):
        self.acknowledged = True
        self._matched_count = matched_count
        self._modified_count = modified_count
        self._upserted_id = upserted_id
        self._raw_result = {
            'n': self._matched_count,
            'nModified': self._modified_count,
            'ok': 1.0,
            'updatedExisting': bool(self._matched_count > 0 and self._modified_count > 0),
        }
        if self._upserted_id is not None:
            self._raw_result['upserted'] = self._upserted_id

    @property
    def matched_count(self):
        return self._matched_count

    @property
    def modified_count(self):
        return self._modified_count

    @property
    def upserted_id(self):
        return self._upserted_id

    @property
    def raw_result(self):
        return self._raw_result
