import hashlib


def person_uri(name_complete, birth_year):
    '''
        args
            name .: string field name_complete for prov=camara

            year .: string 2 digit? or 4 digit? year of birth
                            field dataNascimento for prov=camara
    '''
    year = "";
    if name == "":
        return ""

    if birth_year != "" and birth_year!="99":
        year = birth_year

    splistring = name.split()
    splistring=sorted(splistring)
    id = ""
    for x in splistring:
        id = id + x
    id = id+","+year
    id = id.encode('utf-8');
    
    return "http://www.seliganapolitica.org/resource/"+hashlib.md5(id).hexdigest()