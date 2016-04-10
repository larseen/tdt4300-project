lines = []
astring = " Lorem    upgradable ipsum   do lor sit amet, consectetur  ad  ipisicing elit, sed do upliftingly eiusmod tempor  incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
with open('positive-words.txt', 'r') as text_file:
    lines = text_file.read().split('\n')


alist = astring.split(' ')

print(alist)
