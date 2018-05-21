import json


class Migrator(object):
    '''
        Provides migration utilities in order to convert previous 
            migration schemes into new objects
    '''
    def __init__(self):
        pass

    def migrate(self, file):
        '''
            Migrates file
                * Replace uris for the following resources:
                    * Person
                    * Organization

                * Remove all lines which have uris for resources:
                    * Membership
                    * Post

                * Add instances ttl lines for resources:
                    * Membership
                    * Post
        '''
        pass


class MapperPerson(object):
    def __init__(self):
        '''
            Loads the old and new uri schemas for resource 
        '''
        pass

    def map(self, uriold):
        '''
            maps an uri (old) to a new one

            args:
                uriold : string old uri

            returns:
                urinew : string newly generated uri
        '''
        pass


class MapperOrg(object):
    def __init__(self):
        '''
            Loads the old and new uri schemas for resource 
        '''
        pass

    def map(self, uriold):
        '''
            maps an uri (old) to a new one

            args:
                uriold : string old uri

            returns:
                urinew : string newly generated uri
        '''
        pass