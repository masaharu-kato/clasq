"""
    Sample program 01
"""

from datetime import date as Date

class User:
    name: str
    birthday: Date

class SampleDB:
    users: list[User]


def main():
    db = SampleDB()

    while True:
        cinput = input()
        cmdname, *cmdargs = cinput.split()

        if cmdname == 'exit':
            break

        elif cmdname == 'get':
            for user in db.users:
                print(user.name, user.birthday)

        elif cmdname == 'add':
            db.users.append(User(name=cmdargs[0], birthday=Date.fromisoformat(cmdargs[1])))
            
        else:
            print('Unknown command: %s' % cmdname)


if __name__ == '__main__':
    main()

