from random import randint, choice
from threading import Thread

from controller.Controller import Tags
from servers.neo4j_server.Neo4jServer import Neo4jServer
from servers.redis_server.RedisServer import RedisServer
from faker import Faker

fake = Faker()


class EmulationController(Thread):
    def __init__(self, username, users_list, users_count, loop_count):
        Thread.__init__(self)
        self.__loop_count = loop_count
        self.__server = RedisServer(Neo4jServer())
        self.__users_list = users_list
        self.__users_count = users_count
        self.__server.registration(username)
        self.__user_id = self.__server.sign_in(username)

    def run(self):
        while self.__loop_count > 0:
            message_text = fake.sentence(nb_words=10, variable_nb_words=True, ext_word_list=None)
            receiver = self.__users_list[randint(0, self.__users_count - 1)]
            self.__server.create_message(message_text, self.__get_random_tags(), receiver, self.__user_id)
            self.__loop_count -= 1
        self.stop()

    def __get_random_tags(self) -> list:
        tags = []
        num = randint(0, len(Tags))
        for i in range(num):
            tag = choice(list(Tags)).name
            if tag not in tags:
                tags.append(tag)
        return tags

    def stop(self):
        self.__server.sign_out(self.__user_id)
        self.__loop_count = 0
