import re

person       = re.compile("^.*(firstname|fname|lastname|lname|fullname|maidenname|_name|nickname|name_suffix|name).*$", re.IGNORECASE)
email        = re.compile("^.*(email|e-mail|mail).*$", re.IGNORECASE)
birth_date   = re.compile("^.*(date_of_birth|dateofbirth|dob|birthday|date_of_death|dateofdeath).*$", re.IGNORECASE)
gender       = re.compile("^.*(gender).*$", re.IGNORECASE)
nationality  = re.compile("^.*(nationality).*$", re.IGNORECASE)
address      = re.compile("^.*(address|city|state|county|country|" "zipcode|postal|zone|borough).*$", re.IGNORECASE)
user_name    = re.compile("^.*user(id|name|).*$", re.IGNORECASE)
password     = re.compile("^.*pass.*$", re.IGNORECASE)
ssn          = re.compile("^.*(ssn|social).*$", re.IGNORECASE)

regexes = {
  "person"           : person,
  "email"            : email,
  "birth_date"       : birth_date,
  "gender"           : gender,
  "nationality"      : nationality,
  "address"          : address,
  "user_name"        : user_name,
  "password"         : password,
  "ssn"              : ssn,
}

def column_name_pii_flag(in_string):
    pii_designations = []
    for k, v in list(regexes.items()):
        if v.search(in_string):
            pii_designations.append(k)
    return pii_designations
