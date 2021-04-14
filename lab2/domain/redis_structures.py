# ------- USERS -------
REGULAR_USERS_SET = "regular_users"
ADMIN_USERS_SET = "admin_users"
ONLINE_USERS_SET = "online_users"
# ------- MESSAGES -------
# Stores the id of the latest sent message. Used to generate new ids.
MESSAGE_INDEX = "message_index"
# List with message ids for in-order spam-checks and delivery
MESSAGE_QUEUE = "message_queue"
# Pairs username->[received_message_ids]
INBOUND_MESSAGES_LIST = "inbound_messages"
# Pairs username->[send_message_ids]
OUTBOUND_MESSAGES_LIST = "outbound_messages"
# Pairs message_id->message_object
MESSAGE_HASH = "message"
# Stores ids of enqueued messages foe easy lookup
ENQUEUED_MESSAGES_SET = "messages:enqueued"
# Stores ids of messages marked as spam
SPAM_MESSAGES_SET = "messages:spam"
# Stores ids of messages that are to be spam checked
BEING_SPAM_CHECKED_MESSAGES_SET = "messages:checking_spam"
# Stores ids of messages that were delivered
DELIVERED_MESSAGES_SET = "messages:delivered"
# Keep track of users sending most messages
USERS_BY_DELIVERED_MESSAGES_SORTED_SET = "users_by_delivered_msg"
# Keep track of users who spam the most
USERS_BY_SPAM_MESSAGES_SORTED_SET = "users_by_spam_msg"
# ------- PUB/SUB -------
# Used to log user's sign-in/sign-out and results of spam-checks
EVENT_JOURNAL_CHANNEL = "event_journal"
# Before messages are delivered they're put into a queue for processing
MESSAGE_QUEUE_CHANNEL = "message_queue"
