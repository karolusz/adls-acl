class Kek:
    def __init__(self, oid, value):
        self.v = value
        self.oid = oid

    def __str__(seflf):
        return f"{self.oid}:{self.v}"

    def __eq__(self, other):
        return self.oid == other.oid

    def __hash__(self):
        return hash(self.oid)


if __name__ == "__main__":
    thisset = set(Kek(1, 1), Kek(2, 1), Kek(3, 1))
    mylist = set(Kek(4, 2), Kek(1, 2))

    for item in thisset:
        print(item)

    # thisset.update(mylist)
    mylist.update(thisset)

    print("\n")
    for item in mylist:
        print(item)
