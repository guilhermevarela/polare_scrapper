import hashlib


POLARE_PREFIX_URI = "http://www.seliganapolitica.org/resource/"



def person_uri(fullname, birthdate):
    '''
        args:
            fullname  .: string field fullname for prov=camara

            birthdate .: string representing a date in format YYYY-MM-DD
                            field dataNascimento for prov=camara

        returns: 
            person_uri .: 
        
        usage:
            > _uri = person_uri("CAJAR ONESIMO RIBEIRO NARDES", "1965-11-16")
            > _uri  
            > 'http://www.seliganapolitica.org/resource/24b59d74cbd2d06169557b5bbcdfcdf5'
    '''
    year = ""
    if not fullname:
        return ""

    if birthdate != "" and birthdate != "99":
        year = birthdate

    splistring = fullname.split()
    splistring = sorted(splistring)
    token = ""
    for x in splistring:
        token += x
    token = token + "," + year
    token = token.encode('utf-8')

    return POLARE_PREFIX_URI + hashlib.md5(token).hexdigest()


def membership_party_uri(fullname, uriparty):
    '''
        args:
            fullname   .: string representing name from member
            uriparty   .: string representing uri from party on API_V2

        returns:    

        
        usage:
        > _uri = membership_party_uri("ABEL MESQUITA JR.", "https://dadosabertos.camara.leg.br/api/v2/partidos/36769")
        > _uri
        > http://www.seliganapolitica.org/resource/1ae31f9f97a71c0daa6789a65104b532


    '''
    splistring = uriparty.split()
    splistring = sorted(splistring)
    token = ""
    for x in splistring:
        token += x
    token = fullname + token + "partido" + "membership"
    token = token.encode('utf-8')

    return POLARE_PREFIX_URI + hashlib.md5(token).hexdigest()


def party_uri(nameparty):
    '''
        IS using uri from party and not generated
        args:
            nameparty   .: string representing name from party

        returns:
            slp_prefix + MD5

        usage:
            > _uri = party_uri('Democratas')
            > _uri
            > http://www.seliganapolitica.org/resource/5bc1f31f18f11601c95204de49f1ddee


    '''
    token = str("partido" + nameparty).encode('utf-8')

    return POLARE_PREFIX_URI + hashlib.md5(token).hexdigest()



def post_with_camara_uri(nickname):
    '''
        IS using uri from party

        args:
            nickname .: string representing name from member for camara is nomeParlamentar
            uriorg   .: string representing url from organization

        returns:
        usage:
            #POST AT CAMARA
            > nickname = 'ADAIL CARNEIRO' # fullname = 'JOSE ADAIL CARNEIRO SILVA'
            > _uri = post_with_camara_uri('ADAIL CARNEIRO')
            > _uri
            > http://www.seliganapolitica.org/resource/a983550281d2c166128a9cb63cf00db7

    '''
    # BUGFIX: this section of the code isn't working on original script

    token = ""
    token = nickname + token + "membership" + "post"
    token = token.encode('utf-8')

    return POLARE_PREFIX_URI + hashlib.md5(token).hexdigest()


def post_with_party_uri(nickname, uriorg):
    '''
        IS using uri from party

        args:
            nickname .: string representing name from member for camara is nomeParlamentar
            uriorg   .: string representing url from organization

        returns:
        usage:
            #POST AT PARTY
            > nickname = 'ADAIL CARNEIRO' # fullname = 'JOSE ADAIL CARNEIRO SILVA'
            > _uri = post_uri('ADAIL CARNEIRO', '')
            > _uri 
            > http://www.seliganapolitica.org/resource/a983550281d2c166128a9cb63cf00db7

    '''
    # BUGFIX: this section of the code isn't working on original script
    splistring = uriorg.split()    
    splistring=sorted(splistring)
    token = ""
    for x in splistring:
        token += x
    token = nickname + token + "partido" + "post"
    token = token.encode('utf-8')


    return POLARE_PREFIX_URI + hashlib.md5(token).hexdigest()




if __name__ == '__main__':
    # POSTS AT CAMARA
    assert post_with_camara_uri('ABEL MESQUITA JR.') == 'http://www.seliganapolitica.org/resource/fbc1570e611de1df1da5b46b3e906642'
    assert post_with_camara_uri('ADAIL CARNEIRO') == 'http://www.seliganapolitica.org/resource/a983550281d2c166128a9cb63cf00db7'

    # POSTS AT PARTY    
    assert post_with_party_uri('ADALBERTO CAVALCANTI', 'https://dadosabertos.camara.leg.br/api/v2/partidos/36845') == 'http://www.seliganapolitica.org/resource/8263145ba42b59440d6c6f8d27eda262'
    assert post_with_party_uri('ADELMO CARNEIRO LEÃO', 'https://dadosabertos.camara.leg.br/api/v2/partidos/36844') == 'http://www.seliganapolitica.org/resource/9c1a78ecb33c4ad4b8f896c0a1f231ca'