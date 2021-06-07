from controller.Controller import Controller
from controller.EmulationController import EmulationController
from controller.Neo4jController import Neo4jController
from view import View
from faker import Faker
import random


def emulation():

    fake = Faker()
    users_count = 5
    users = [fake.profile(fields=['username'], sex=None)['username'] for u in range(users_count)]
    threads = []
    try:

        for i in range(users_count):
            threads.append(EmulationController(users[i], users, users_count, random.randint(1, 2)))
        for thread in threads:
            thread.start()

    except Exception as e:
        View.show_error(str(e))
    finally:
        for thread in threads:
            if thread.is_alive():
                thread.stop()


if __name__ == "__main__":
    choice = Controller.make_choice(["Использовать Neo4j", "Емуляция"], "Выберите: ")
    if choice == 0:
        Neo4jController()
    elif choice == 1:
        emulation()
