from controller.Neo4jController import Neo4jController
from controller.Controller import Controller, Tags
from servers.neo4j_server.Neo4jServer import Neo4jServer

menu_list = {
    'Вибрати': {
        'Знайти всіх користувачів, що відправили або отримали повідомлення з набором тегів tags': Neo4jController.get_users_with_tagged_messages,
        'Знайти усі пари користувачів, що мають зв’язок довжиною N через відправлені або отримані повідомлення': Neo4jController.get_users_with_n_long_relations,
        'Знайти на графі найкоротший шлях між двума користувачами через відправлені або отримані повідомлення': Neo4jController.shortest_way_between_users,
        'Знайти авторів повідомлень, які пов’язані між собою лише повідомленнями, позначеними як “спам”.': Neo4jController.get_users_wicth_have_only_spam_conversation,
        'Знайти всіх користувачів, що відправили або отримали повідомлення з набором тегів tags, але ці користувачі не пов’язані між собою.': Neo4jController.get_unrelated_users_with_tagged_messages,
        'Назад': Controller.stop_loop,
    }
}

roles = {
    'utilizer': 'Utilizer menu',
    'admin': 'Admin menu'
}

neo4j = Neo4jServer()
special_parameters = {
    'role': '(admin or utilizer)',
    'tags': '('+', '.join(x.name for x in list(Tags))+')',
    'username1': '(' + ', '.join(x for x in neo4j.get_users()) + ')',
    'username2': '(' + ', '.join(x for x in neo4j.get_users()) + ')'
}
