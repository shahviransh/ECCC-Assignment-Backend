import h5py
import numpy
import numpy as np
from pymongo import MongoClient
from gridfs import  GridFSBucket

# The HDF5 file path is 'path_to_your_file.hdf5'
# And what is the name of the dataset you want to access is 'subbasin'
file_path = 'parameter.h5'
dataset_name = 'subbasin'

# use h5py open file
hdf_file= h5py.File(file_path, 'r')
asc=hdf_file["asc"]
key=asc.keys()

dataset = numpy.array(asc[dataset_name],np.float32)
attributes = asc.attrs
for attr_name, attr_value in attributes.items():
    print(f"  {attr_name}: {attr_value}")
ncols=attributes["NCOLS"]
nrows=attributes["NROWS"]
xllcorner=attributes["XLLCENTER"]
yllcorner=attributes["YLLCENTER"]
cellsize=attributes["DX"]
NODATA_value=attributes["NODATA_VALUE"]
