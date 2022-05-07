"""
    Ordered Set
"""

class OrderedSet:
    
    def __init__(self, _iterable) -> None:
        """ Init """
        self._dict = {v: None for v in _iterable}

    def __contains__(self, val):
        return val in self._dict
        
    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def __and__(self, oset):
        return OrderedSet(v for v in self._dict if v in oset)

    def __or__(self, oset):
        return OrderedSet((*self._dict, *(oset - self._dict)))

    def __sub__(self, oset):
        return OrderedSet(v for v in self._dict if v not in oset)

    def __iand__(self, oset):
        for v in self._dict:
            if v not in oset:
                del self._dict[v]
        return self

    def __ior__(self, oset):
        for v in oset:
            self.add(v)
        return self

    def __isub__(self, oset):
        for v in self._dict:
            if v in oset:
                self.remove(v)
        return self
        
    def add(self, val) -> None:
        if val not in self._dict:
            self._dict[val] = None

    def update(self, *vals_list) -> None:
        for objs in vals_list:
            self.__ior__(objs)

    def remove(self, val) -> None:
        del self._dict[val]
