
import pandas as pd
import numpy as np

import matplotlib as plt

############################################ SERIES
s1 = pd.Series([1,2,3])
print(s1)

s1 = pd.Series([1, 2, 3], index=["a","b","c"])
print(s1)

s1 = pd.Series([1, 2, 3], index=["a","b","c"])
print(s1[['a','b']])

s2= pd.Series({"a1":"1","b1":"2","d1":"3"})
print(s2)


# Operations

s1 = pd.Series([1,2,3], index=["a","b","c"]);
s2 = pd.Series([1,2,3], index=["a","c","d"]);

print(s1+s2)



############################################ DATA FRAME

s1 = pd.DataFrame(data=  [[1,2,3], [4,5,6],[7,8,9]],
                  columns= [ "C1","C2","C3"],
                  index= [ "R1","R2","R3"]
                  )
print(s1.head(2))

s1["newCol"] = [10,11,12]
print(s1)

s1["newCol"] = s1["C1"] + 4
print(s1)

s1.drop("newCol", axis=1, inplace=True)
print(s1)

s1.drop("R3", axis=0, inplace=True)
print(s1)

## Selection
s2 = pd.DataFrame(data=  [[1,2,3], [4,5,6],[7,8,9]],
                  columns= [ "C1","C2","C3"],
                  index= [ "R1","R2","R3"])

s3 = s2[["C1","C2"]]
print(s3)

s4 = s2.loc["R2":,:"C2"]
print(s4)

s5 = s2.iloc[ 1:,1:3 ]
print(s5)


######################################## Indexing

s1 = pd.DataFrame(data=  [[1,2,3,10], [4,5,6,11],[7,8,9,12]],
                  columns= [ "C1","C2","C3","C4"],
                  index= [ "R1","R2","R3"])
s1.reset_index()
print(s1)
print(s1.index)
print(s1.iloc[1:,:2])


s1.set_index("C1" ,inplace=True)
print(s1)
print(s1.index)


######################################## Missing Data

s1 = pd.DataFrame([[1,2,np.nan],[4,5,np.nan],[np.nan,8,np.nan],[10,11,np.nan]], columns=list("abc"))
print(s1)

s1.dropna(axis=1,how="all",inplace=True)
print(s1)

s1.dropna(axis=0,how="any",inplace=True)
print(s1)

s2 = pd.DataFrame([[1,2,np.nan],[4,5,np.nan],[np.nan,8,np.nan],[10,11,np.nan]], columns=list("abc"))
s2.fillna(s2["a"].mean(), inplace=True)
print(s2)

######################################### Group By

s1 = pd.DataFrame(data=[ ["MI","P1","Male",54,15],
                        ["MI","P1","Male",54,15],
                        ["DD","P2","Female",54,15],
                        ["RR","P2","Male",54,15],
                        ["MI","P3","Female",54,15],
                        ["DS","P3","Male",54,15],
                        ["DS","P4","Female",54,15],
                        ["MI","P5","Male",54,15],
                        ["CS","P8","Female",54,15],
                        ["CS","P8","Male",54,15],
                        ["MI","P7","Female",54,15]],
                  columns=['Team','Player','Sex','Score','Age'])

print(s1['Team'].value_counts())

print(s1['Age'].sum())
print(s1['Team'].unique())

print(s1.groupby('Team').mean())

print(s1.groupby('Team').describe())

def myname(x):
    if x>15:
        return "Very Good, "

print(s1["Score"].apply(myname) + s1["Player"])

##################################### Read File

s1 = pd.read_csv("emp.csv" )
print(s1)

######################## Visualize the Data

s1 = pd.DataFrame(data=  [[1,2,3], [4,5,6],[7,8,9]],
                  columns= [ "C1","C2","C3"]
                  )
print(s1)

s1.plot.area();


