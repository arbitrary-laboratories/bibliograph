class SchemaObject(object):
    def __init__(self, description, schema_json):
        self.description = description
        # "description"
        self.schema_json = schema_json
        # [{'mode': 'NULLABLE',
        #   'name': 'test_column_1',
        #   'type': 'STRING',
        #   'description': 'The first test column'},
        #  {'mode': 'NULLABLE',
        #   'name': 'test_column_2',
        #   'type': 'INTEGER',
        #   'description': 'The second test column'},
        #  {'mode': 'NULLABLE',
        #   'name': 'test_column_3',
        #   'type': 'TIMESTAMP',
        #   'description': 'The third test column'}]
