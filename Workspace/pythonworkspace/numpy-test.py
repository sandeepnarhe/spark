
import numpy as np

# Array to Numpy
vec = [ 1,2,3]
np_vec = np.array(vec)
print(np_vec)

# Array to Numpy 2d
vec2d = [[1,2,3],[4,5,6]]
np_vec2d = np.array(vec2d)
print(np_vec2d)

#Ones
np_ones = np.ones((3, 4))
print(np_ones)

#Zeros
np_zeros = np.zeros((3, 4))
print(np_zeros)

# Identity Matrix
np_identity = np.identity(4)
print(np_identity)

# Random Number
np_randnumber = np.random.randn(3)
print(np_randnumber)

# Random Number
np_randint = np.random.randint(3,90,3)
print(np_randint)

print(np_zeros.reshape(12,1))


############# Operations ###########################


a1 = np.array([[1,2,3], [ 4,5,6]])
b1 = np.array([[2,2,2],[3,3,3]])
print(a1*b1)
print(a1*2)
print(a1/2)
print(a1>2)

############## Indexing #################

a1= np.arange(25).reshape(5,5)
print(a1)
print(a1[2:4,2:3])
