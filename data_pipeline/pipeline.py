
import os
root_directory = '/data'

# INPUT: raw attribute list file (raster metadata)

attribute_raw_file = os.path.join(root_directory, 'CSV/attribute_list_raw_test.csv')

# OUTPUT: updated attribute list file
attribute_update_file = os.path.join(root_directory + '/CSV/attribute_list_updated_test.csv')


# INPUT: vector list file
vector_file = os.path.join(root_directory + '/CSV/vector_list.csv')

# INPUT: SIDS list file
sids_list_file = os.path.join(root_directory + '/CSV/sids_list_extended.csv')
