import logging
import os
from data_pipeline.standardization import standardize


logger = logging.getLogger()


if __name__ == '__main__':

    logging.basicConfig()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))

    # remove the default stream handler and add the new on too it.
    logger.handlers.clear()
    logger.addHandler(sthandler)

    logger.setLevel('INFO')
    logger.name = os.path.split(__file__)[-1]

    #0 IO folders
    root_directory = '/data/sids'
    if not os.path.exists(root_directory):
        os.mkdir(root_directory)
    #
    #
    # from_disk = True
    #
    #
    # if from_disk:
    #     # INPUT: raw attribute list file (raster metadata)
    #     attribute_raw_file = os.path.join(root_directory, 'CSV/attribute_list_raw_test.csv')
    #
    #     # OUTPUT: updated attribute list file
    #     attribute_update_file = os.path.join(root_directory + '/CSV/attribute_list_updated_test.csv')
    #     # INPUT: vector list file
    #     vector_file = os.path.join(root_directory + '/CSV/vector_list.csv')
    #
    #     # INPUT: SIDS list file
    #     sids_list_file = os.path.join(root_directory + '/CSV/sids_list_extended.csv')

    else:
        attribute_raw_file = 'https://drive.google.com/file/d/1_kL7Iq4yFus4DKbgbsw6yUjMKn7QsB7o/view?usp=sharing'
        attribute_update_file = 'https://drive.google.com/file/d/178N0wc2SUHqIO2iwQIUUALbO0q3oPOCb/view?usp=sharing'
        vector_file = 'https://drive.google.com/file/d/10NF0PDFSML1tWfybT8zu5aldHv0CRoHZ/view?usp=sharing'
        sids_list_file = 'https://drive.google.com/file/d/1Z2U1oRm7nB6WyhJiSQcg3Mw0QVqH7Z7U/view?usp=sharing'

    dst_folder = os.path.join(root_directory, 'standardized_data')

    # 1 STANDARDIZE

    a = standardize(dst_folder=dst_folder,
                    attribute_raw_file=attribute_raw_file
                    )







