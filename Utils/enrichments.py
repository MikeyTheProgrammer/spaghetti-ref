import hashlib

#This function returns the hash value for a given x variable
def hashfunc(x):

    if x == '' or x is None or len(x) == 0:
        return ''

    to_hash = str(x).encode()
    hash_ = hashlib.sha1(to_hash)
    hash_ = hash_.hexdigest()
    return hash_