import pika
import requests

ENDPOINTS = {
    'definitions': 'api/definitions',
    'queues': 'api/queues',
    'shovel': 'api/parameters/shovel/%2F/{shovel_name}', # For simplicity, we are not support dynamic vhosts for now
    'shovels': 'api/shovels/%2F'
}

class RabbitMQAdmin(object):

    def __init__(self, host='localhost', port='15672', user='guest', passwd='guest'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.base = 'http://%s:%s'%(host, port)
        self.auth = requests.auth.HTTPBasicAuth(user, passwd)


    def get_definitions(self):
        res = self.__api_get(uri=ENDPOINTS.get('definitions'))

        if res.status_code == 200:
            return res.json()

        else:
            print('Error to get definitions', res.text)
            return {}


    def set_definitions(self, definitions):

        if not (isinstance(definitions, dict) or isinstance(definitions, requests.models.Response)):
            print('Invalid definitions', definitions)
            res = requests.models.Response()
            res.status_code = 415
            res.reason = "Unsupported definitions type"
            return res

        defs = definitions if isinstance(definitions, dict) else definitions.json()
        res = self.__api_post(defs, uri=ENDPOINTS.get('definitions'))

        if res.status_code > 300:
            print('Error to set definitions', res.text)

        return res


    def create_shovels(self, from_rabbit_instance, exclude_empty=True):

        if not isinstance(from_rabbit_instance, RabbitMQAdmin):
            return

        queues = from_rabbit_instance.get_queues(exclude_empty=exclude_empty)

        if len(queues) > 0:
            for q in queues:
                self.create_queue_shovel(q, from_rabbit_instance.host, self.host)

        else:
            print('No queues found')


    def delete_shovels(self):
        uri = ENDPOINTS.get('shovels')
        res = self.__api_get(uri=uri)

        if res.status_code == 200:
            for shovel in res.json():
                self.delete_shovel(shovel.get('name'))

        else:
            print('Error to get shovels list', res.text)


    def delete_shovel(self, shovel_name=None):
        if not shovel_name:
            return False

        shovel_uri = ENDPOINTS.get('shovel').format(shovel_name = shovel_name)
        res = self.__api_delete(uri=shovel_uri)

        if res.status_code > 300:
            print('Error to delete shovel', shovel_name, res.text)

        return res


    def create_queue_shovel(self, queue_name, src_host, dest_host, src_port='5672', dest_port='5672'):
        if not queue_name:
            return False

        shovel_name = "shovel-%s"%queue_name
        payload = {
            "component": "shovel",
            "name": shovel_name,
            "value": {
                "src-uri": "amqp://%s:%s"%(src_host, src_port),
                "src-queue": queue_name,
                "dest-uri": "amqp://%s:%s"%(dest_host, dest_port),
                "dest-queue": queue_name,
                "ack-mode": "on-confirm",
                "add-forward-headers": False,
                "delete-after": "never",
                "prefetch-count": 0,
                "reconnect-delay": 5
            }
        }

        shovel_uri = ENDPOINTS.get('shovel').format(shovel_name = shovel_name)
        res = self.__api_put(payload, uri=shovel_uri)

        if res.status_code > 300:
            print('Error to create queue shovel', queue_name, res.text)

        return res


    def get_queues(self, exclude_empty=False):
        res = self.__api_get(ENDPOINTS.get('queues'))

        if res.status_code > 300:
            print('Error to get queues', res.text)
            return []

        queues = []
        rqueues = res.json()

        if exclude_empty:
            queues = [q.get('name') for q in rqueues if (q.get('messages') and q.get('messages') > 0)]
        else:
            queues = [q.get('name') for q in rqueues]

        return queues


    def __api_get(self, uri=None):
        if uri is None:
            return None

        url = '/'.join([self.base, uri])
        return requests.get(url, auth=self.auth)


    def __api_post(self, payload, uri=None):
        if uri is None:
            return None

        url = '/'.join([self.base, uri])
        return requests.post(url, auth=self.auth, json=payload)


    def __api_put(self, payload, uri=None):
        if uri is None:
            return None

        url = '/'.join([self.base, uri])
        return requests.put(url, auth=self.auth, json=payload)


    def __api_delete(self, uri=None):
        if uri is None:
            return None

        url = '/'.join([self.base, uri])
        return requests.delete(url, auth=self.auth)
