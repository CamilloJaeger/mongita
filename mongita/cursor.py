from .errors import MongitaNotImplementedError, MongitaError, InvalidOperation
from .common import ASCENDING, DESCENDING, support_alert


def _validate_sort(key_or_list, direction=None):
    """
    Validate kwargs and return a proper sort list

    :param key_or_list str|[(str key, int direction), ...]
    :param direction int:
    :rtype: [(str key, int direction), ...]
    """
    if direction is None and isinstance(key_or_list, (list, tuple)) \
       and all(isinstance(tup, (list, tuple)) and len(tup) == 2 for tup in key_or_list):
        _sort = key_or_list
    elif direction is None and isinstance(key_or_list, str):
        _sort = [(key_or_list, ASCENDING)]
    elif isinstance(key_or_list, str) and isinstance(direction, int):
        _sort = [(key_or_list, direction)]
    else:
        raise MongitaError("Unsupported sort parameter format. See the docs.")
    for sort_key, sort_direction in _sort:
        if not isinstance(sort_key, str):
            raise MongitaError("Sort key(s) must be strings %r" % str(key_or_list))
        if sort_direction not in (ASCENDING, DESCENDING):
            raise MongitaError("Sort direction(s) must be either ASCENDING (1) or DESCENDING (-1). Not %r" % direction)
    return _sort


class Cursor():
    UNIMPLEMENTED = ['add_option', 'address', 'alive', 'allow_disk_use', 'batch_size',
                     'collation', 'comment', 'cursor_id', 'distinct',
                     'explain', 'hint', 'max', 'max_await_time_ms',
                     'max_time_ms', 'min', 'remove_option', 'retrieved', 'rewind',
                     'session', 'where']
    DEPRECATED = ['max_scan']

    def __init__(self, _find, filter, sort, limit, skip):
        self._find = _find
        self._filter = filter
        self._sort = sort or []
        self._limit = limit or None
        self._skip = skip or None
        self._cursor = None

    @property
    def collection(self):
        """
        The :class:`~mongita.collection.Collection` that this
        :class:`~mongita.cursor.Cursor` is iterating.
        """
        return self._find.__self__

    def __getattr__(self, attr):
        if attr in self.DEPRECATED:
            raise MongitaNotImplementedError.create_depr("Cursor", attr)
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create("Cursor", attr)
        raise AttributeError()

    def __getitem__(self, key):
        if self._cursor:
            raise InvalidOperation("Cannot slice a cursor that has already been used.")

        if isinstance(key, slice):
            if key.step is not None and key.step != 1:
                raise IndexError("Cursor slicing does not support step.")

            new_cursor = self.clone()

            start = key.start or 0
            if start < 0:
                raise IndexError("Negative indices are not supported.")

            new_cursor._skip = (self._skip or 0) + start

            if key.stop is not None:
                if key.stop < 0:
                    raise IndexError("Negative indices are not supported.")

                if key.stop <= start:
                    new_cursor._limit = 0
                    return new_cursor

                slice_len = key.stop - start

                if self._limit is not None:
                    if start >= self._limit:
                        new_cursor._limit = 0
                    else:
                        new_cursor._limit = min(slice_len, self._limit - start)
                else:
                    new_cursor._limit = slice_len
            else:  # key.stop is None
                if self._limit is not None:
                    if start >= self._limit:
                        new_cursor._limit = 0
                    else:
                        new_cursor._limit = self._limit - start

            return new_cursor

        if isinstance(key, int):
            if key < 0:
                raise IndexError("Negative indices are not supported.")

            c = self.clone()
            if self._limit is not None and key >= self._limit:
                raise IndexError("Cursor index out of range.")

            c._skip = (self._skip or 0) + key
            c._limit = 1
            docs = list(c)
            if not docs:
                raise IndexError("Cursor index out of range.")
            return docs[0]

        raise TypeError(f"Cursor indices must be integers or slices, not {type(key).__name__}")

    def __iter__(self):
        for el in self._gen():
            yield el

    def __next__(self):
        return next(self._gen())

    def _gen(self):
        """
        This exists so that we can maintain our position in the cursor and
        to not execute until we start requesting items
        """
        if self._cursor:
            return self._cursor
        self._cursor = self._find(filter=self._filter, sort=self._sort,
                                  limit=self._limit, skip=self._skip)
        return self._cursor

    @support_alert
    def next(self):
        """
        Returns the next document in the Cursor. Raises StopIteration if there
        are no more documents.

        :rtype: dict
        """
        return next(self._gen())

    @support_alert
    def count(self, with_limit_and_skip=False):
        """
        DEPRECATED: Use collection.count_documents instead.
        Counts the number of documents in this cursor's result set.
        :param with_limit_and_skip bool: If True, respects limit and skip.
        :rtype: int
        """
        if self._cursor:
            raise InvalidOperation("Cannot count a cursor that has already been used.")

        if with_limit_and_skip:
            return len(list(self.clone()))

        return self.collection.count_documents(self._filter)

    @support_alert
    def sort(self, key_or_list, direction=None):
        """
        Apply a sort to the cursor. Sorts have no impact until retrieving the
        first document from the cursor. If not sorting against indexes, sort can
        negatively impact performance.
        This returns the same cursor to allow for chaining. Only the last sort
        is applied.

        :param key_or_list str|[(key, direction)]:
        :param direction mongita.ASCENDING|mongita.DESCENDING:
        :rtype: cursor.Cursor
        """

        self._sort = _validate_sort(key_or_list, direction)
        if self._cursor:
            raise InvalidOperation("Cursor has already started and can't be sorted")

        return self

    @support_alert
    def limit(self, limit):
        """
        Apply a limit to the number of elements returned from the cursor.
        This returns the same cursor to allow for chaining. Only the last limit
        is applied.

        :param limit int:
        :rtype: cursor.Cursor
        """
        if not isinstance(limit, int):
            raise TypeError('Limit must be an integer')

        if self._cursor:
            raise InvalidOperation("Cursor has already started and can't be limited")

        self._limit = limit
        return self

    @support_alert
    def skip(self, skip):
        """
        Skip the first [skip] results of this cursor.
        """
        if not isinstance(skip, int):
            raise TypeError("The 'skip' parameter must be an integer")
        if skip < 0:
            raise ValueError("The 'skip' parameter must be >=0")
        if self._cursor:
            raise InvalidOperation("Cursor has already started and skip can't be applied")

        self._skip = skip
        return self

    @support_alert
    def clone(self):
        return Cursor(self._find, self._filter, self._sort, self._limit, self._skip)

    @support_alert
    def close(self):
        """
        Close this cursor to free the memory
        """
        self._cursor = iter(())