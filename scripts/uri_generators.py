import hashlib


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
    year = "";
    if fullname == "":
        return ""

    if birthdate != "" and birthdate!="99":
        year = birthdate

    splistring = fullname.split()
    splistring=sorted(splistring)
    id = ""
    for x in splistring:
        id = id + x
    id = id+","+year
    id = id.encode('utf-8');
    
    return "http://www.seliganapolitica.org/resource/" + hashlib.md5(id).hexdigest()



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

    return "http://www.seliganapolitica.org/resource/" + hashlib.md5(token).hexdigest()


def membership_party_uri(fullname, uriparty):
    '''
        args:
            fullname   .: string representing name from party
            uriparty    .: string representing uri from party on API_V2

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
    print(token)
    return "http://www.seliganapolitica.org/resource/" + hashlib.md5(token).hexdigest()


def party_uri(nameparty):
    '''
        IS using uri from party and not generated
        args:
            nameparty   .: string representing name from party

        returns:    

        
        usage:
        > _uri = party_uri('Democratas')
        > _uri
        > http://www.seliganapolitica.org/resource/5bc1f31f18f11601c95204de49f1ddee


    '''    
    token = str("partido" + nameparty).encode('utf-8')

    return "http://www.seliganapolitica.org/resource/" + hashlib.md5(token).hexdigest()