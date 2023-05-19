## Include extra data cleaning 

### More than one person cannot have the same number - duplicates recognised via subset - kept the first instance and deleted others
        users = users.set_index('join_date')
        users = users.sort_index(ascending=True)
        print(f"Joined {users}")
        duplicated_phone = users.duplicated(subset=['phone_number'], keep = 'first')
        users = users[~duplicated_phone]
        users = users.reset_index()
        print(users)