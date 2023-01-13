

def unique_list(l):
    ulist = []
    for i in l:
        if len(i) > 0:
            if i not in ulist:
                ulist.append(i)

    ulist.sort()

    return ulist

def unique_list2(l):
    ulist = []
    for i in l:
        if len(i) > 0:
                ulist.append(i)

    return ulist