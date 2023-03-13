class Columns:
    historic_node_data = [
                    'index timestamp',
                    'date',
                    'time',
                    'connectivity',
                    'data collect time',
                    'discord handle',
                    'discord id',
                    'node ip',
                    'node port',
                    'node id',
                    'node wallet',
                    'node balance',
                    'node state',
                    'node version',
                    'node cpu count',
                    'node 1m load average',
                    'high load',
                    'node free disk space',
                    'node total disk space',
                    'free disk space percent',
                    'low disk space',
                    'source state',
                    'source version',
                    'cluster name',
                    'layer',
                    'node peer count',
                    'source peer count',
                    'association time',
                    'dissociation time',
                    'node latency',
                    'node session',
                    'source session',
                    'shared session',
                    'explorer url',
                    'source ip',
                    'source port',
                    'source id'
                ]

    subscribers = ['name',
                   'contact',
                   'ip',
                   'public_l0',
                   'public_l1',
                   'node_id'
                   ]

    load_balancers = {
        'name',
        'url',
        'id'
    }


class Dtypes:
    historic_node_data = {
                        'index timestamp': 'int',
                        'data': 'str',
                        'time': 'str',
                        'data collect time': 'int',
                        'discord handle': 'str',
                        'discord id': 'int',
                        'node ip': 'str',
                        'node port': 'int',
                        'node id': 'str',
                        'node wallet': 'str',
                        'node balance': 'float',
                        'node state': 'str',
                        'node version': 'str',
                        'node cpu count': 'float',
                        'node 1m load average': 'float',
                        'high load': 'bool',
                        'node free disk space': 'float',
                        'node total disk space': 'float',
                        'free disk space percent': 'float',
                        'low disk space': 'bool',
                        'source state': 'str',
                        'source version': 'str',
                        'cluster name': 'str',
                        'layer': 'int',
                        'node peer count': 'int',
                        'source peer count': 'int',
                        'association time': 'int',
                        'dissociation time': 'int',
                        'node latency': 'int',
                        'node session': 'int',
                        'source session': 'int',
                        'shared session': 'bool',
                        'explorer url': 'str',
                        'source ip': 'str',
                        'source port': 'int',
                        'source id': 'str'
                    }

    subscribers = {
        'name': 'str',
        'contact': 'str',
        'ip': 'str',
        # must be str because it can be empty
        'public_l0': 'str',
        'public_l1': 'str',
        'node_id': 'str'
    }

    load_balancers = {
        'name': 'str',
        'url': 'str',
        'id': 'str'
    }
