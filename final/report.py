import pandas as pd
import numpy as np


def report_diff(x):
    return x[0] if x[0] == x[1] else '{} ---> {}'.format(*x)


old=pd.read_json("/home/ashok/data-1482390767795.txt")
new=pd.read_json("/home/ashok/ashok.txt")    
old['version'] = "old"
new['version'] = "new"

full_set = pd.concat([old,new],ignore_index=True)


changes = full_set.drop_duplicates(subset=["buyable", "keyword", "name", "shortDescription", "uniqueID"],take_last=True)

dupe_accts = changes.set_index('uniqueID','name').index.get_duplicates()


dupes = changes[changes["uniqueID"].isin(dupe_accts)]


change_new = dupes[(dupes["version"] == "new")]
change_old = dupes[(dupes["version"] == "old")]


change_new = change_new.drop(['version'], axis=1)
change_old = change_old.drop(['version'], axis=1)


change_new.set_index('uniqueID',inplace=True)
change_old.set_index('uniqueID',inplace=True)


diff_panel = pd.Panel(dict(df1=change_old,df2=change_new))
diff_output = diff_panel.apply(report_diff, axis=0)


changes['duplicate']=changes["uniqueID"].isin(dupe_accts)


removed_accounts = changes[(changes["duplicate"] == False) & (changes["version"] == "old")]



new_account_set = full_set.drop_duplicates(subset=['buyable','keyword', 'manufacturer', 'name','shortDescription', 'uniqueID'],take_last=False)


new_account_set['duplicate']=new_account_set["uniqueID"].isin(dupe_accts)


added_accounts = new_account_set[(new_account_set["duplicate"] == False) & (new_account_set["version"] == "new")]

print(added_accounts)
print(diff_output)
writer = pd.ExcelWriter("/home/ashok/my-diff-2.xlsx")
diff_output.to_excel(writer,"changed")
removed_accounts.to_excel(writer,"removed",index=False,columns=['buyable','keyword', 'manufacturer', 'name','shortDescription', 'uniqueID'])
added_accounts.to_excel(writer,"added",index=False,columns=['buyable','keyword', 'manufacturer', 'name','shortDescription', 'uniqueID'])
writer.save()

