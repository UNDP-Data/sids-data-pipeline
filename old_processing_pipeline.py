# SIDS Geospatial Dashboard Processing Pipeline
# (Steps matching workflow in manual)
# Step 3: Standardization
# Step 4: Vectorization
# Step 5: Coverage Calculation
# (Run in QGIS GUI or Python 3 with QGIS packages imported)

# root directory, including folders: Data, Shapefile, CSV
root_directory = r"D:\UNDP\Atlas"

# INPUT: raw attribute list file (raster metadata)
attribute_raw_file = root_directory + r"\CSV\attribute_list_raw_test.csv"

# OUTPUT: updated attribute list file
attribute_update_file = root_directory + r"\CSV\attribute_list_updated_test.csv"

# INPUT: vector list file
vector_file = root_directory + r"\CSV\vector_list.csv"

# INPUT: SIDS list file
sids_list_file = root_directory + r"\CSV\sids_list_extended.csv"

import csv

def standardization():    

    # directory to store standardized raster files (make sure there is enough space)
    # EXISTING CONTENTS WILL BE DELETED!
    working_directory = root_directory + r"\3_Standardization"

    # ----- Adjust Parameters Above -----
    
    print("\nStart standardization...")

    # create folder
    if os.path.exists(working_directory):
        list( map( os.unlink, (os.path.join(working_directory,f) for f in os.listdir(working_directory)) ) )
    else:
        os.makedirs(working_directory)
        
    # read attribute file
    attributes = []
    f_in = open(attribute_raw_file,'r',encoding='latin1')
    header = f_in.readline().strip().split(",")
    reader = csv.reader(f_in)
    for row in reader:
        attributes.append(row)
    f_in.close()

    # output file
    f_out = open(attribute_update_file,'w', encoding='latin1',newline='')
    writer = csv.writer(f_out)
    writer.writerow(header)
        
    # iterate each attribute
    for attribute in attributes:
        
        # init
        id = attribute[0]
        print("\nAttribute:",id)
        input_raster_file = root_directory + "\\Data\\" + attribute[1]+"\\"+attribute[2]
        band = int(attribute[3])
        
        # read projection information
        fileName = input_raster_file
        fileInfo = QFileInfo(input_raster_file)
        baseName = fileInfo.baseName()
        rlayer = QgsRasterLayer(fileName, baseName)
        crs = rlayer.crs().authid()
        print ("CRS: ",crs,"#band: ",rlayer.bandCount())
        
        # re-project
        if crs!='EPSG:4326':
            print ("Re-project!")
            params_wrap={
                'INPUT' : input_raster_file,
                'SOURCE_CRS': rlayer.crs(),
                'TARGET_CRS':'EPSG:4326',
                'OUTPUT':working_directory+"\\"+id+"_proj.tif"
            }
            result = processing.run("gdal:warpreproject", params_wrap)
            input_raster_file = QgsRasterLayer(result['OUTPUT'])
            attribute[1] = working_directory
            attribute[2] = id+"_proj.tif"
        
        # re-arrange band (for non-tif and multi-band file)
        if ((attribute[2][-3:]!="tif")and(attribute[2][-4:]!="tiff"))or(rlayer.bandCount()>1):
            print ("Re-arrange Band!")
            params_band={
                'BANDS' : [band],
                'DATA_TYPE' : 0, 
                'INPUT' : input_raster_file, 
                'OPTIONS' : '', 
                'OUTPUT' : working_directory+"\\"+id+"_band.tif"
            }
            result = processing.run("gdal:rearrange_bands", params_band)
            input_raster_file = QgsRasterLayer(result['OUTPUT'])
            attribute[1] = r"3_Standardization"
            attribute[2] = id+"_band.tif"
            attribute[3] = 1
        
        writer.writerow(attribute)

    f_out.close()
    print("Finish standardization!")
    
def vectorization(vector_layer,data_type):  
    
    layer_list = ["admin0","admin1","admin2",\
        "hex-10km","hex-5km","hex-1km","hex-10km-ocean",\
        "grid-10km","grid-5km","grid-1km","grid-10km-ocean"]
        
    layer_type = layer_list.index(vector_layer)
    # 1 -> admin1
    # 2 -> admin2
    # 3 -> 10km hex
    # 4 -> 5km hex
    # 5 -> 1km hex
    # 6 -> 10km hex (ocean)
    # 7 -> 10km grid
    # 8 -> 5km grid
    # 9 -> 1km grid
    # 10 -> 10km grid (ocean)

    # zonal statistics algorithm
    if data_type == "quantitative":
        algorithm == 2 # mean (average)
    elif data_type == "qualitative":
        algorithm == 9 # majority (mode)
    else:
        algorithm == 9
        print ("ERROR: Invalid data type! (treated as qualitative)")        
        

    # field length and precision
    field_length = 9
    field_precision = 4

    # directory to store standardized raster files (make sure there is enough space)
    # EXISTING CONTENTS WILL BE DELETED!
    working_directory = root_directory + r"\4_Vectorization"

    # ----- Adjust Parameters Above -----

    print("\nStart vectorization (",layer_type,",",algorithm,")...")
    
    attribute_file = attribute_update_file

    # create folder
    if not(os.path.exists(working_directory)):
        os.makedirs(working_directory)

    # read vector file
    vectors = []
    f_in = open(vector_file,'r',encoding='latin1')
    header = f_in.readline().strip().split(",")
    reader = csv.reader(f_in)
    for row in reader:
        vectors.append(row)
    f_in.close()
    input_layer = root_directory + "\\" + vectors[layer_type][1]+"\\"+vectors[layer_type][2]

    # create sub-folder
    working_directory = working_directory + "\\" + vectors[layer_type][0]
    if (os.path.exists(working_directory)):
        list( map( os.unlink, (os.path.join( working_directory,f) for f in os.listdir(working_directory)) ) )
    else:
        os.makedirs(working_directory)
        
    # define field
    field_list=[]
    if layer_type == 1: #admin1
        field_list.extend([
    {'expression': '\"GID_0\"','length': 3,'name': 'GID_0','precision': 0,'type': 10},
    {'expression': '\"GID_1\"','length': 8,'name': 'GID_1','precision': 0,'type': 10}
    ])
    elif layer_type == 2: #admin2
        field_list.extend([
    {'expression': '\"GID_0\"','length': 3,'name': 'GID_0','precision': 0,'type': 10},
    {'expression': '\"GID_2\"','length': 11,'name': 'GID_2','precision': 0,'type': 10}
    ])
    elif layer_type <= 6: #hexagon
        field_list.append(
    {'expression': '\"hex_id\"','length': 11,'name': 'hex_id','precision': 0,'type': 10})
    else: #grid
        field_list.append(
    {'expression': '\"grid_id\"','length': 11,'name': 'grid_id','precision': 0,'type': 10})

    # read attribute file
    attributes = []
    f_in = open(attribute_file,'r',encoding='latin1')
    header = f_in.readline().strip().split(",")
    reader = csv.reader(f_in)
    for row in reader:
        attributes.append(row)
        field_dict={'expression': '\"'+row[0]+'\"','length': field_length,'name': row[0],'precision': field_precision,'type': 6}
        field_list.append(field_dict)
    f_in.close()

    # iterate each attribute (raster data)
    for row in attributes:

        # init and parameters
        id = row[0]
        print("\nAttribute:",id)
        input_raster = root_directory+"\\"+row[1]+"\\"+row[2]
        output_layer = working_directory+"\\"+id+".shp"
        params={ 
            'COLUMN_PREFIX' : '_', 
            'INPUT' : input_layer, 
            'INPUT_RASTER' : input_raster,
            'OUTPUT' : output_layer, 
            'RASTER_BAND' : int(row[3]), 
            'STATISTICS' : [algorithm]
            }
        
        print (input_layer,input_raster,output_layer,int(row[3]))
        # zonal analysis
        result = processing.run("native:zonalstatisticsfb", params)
        layer = QgsVectorLayer(result['OUTPUT'])

        # rename new field
        new_field_name = id
        for field in layer.fields():
            if field.name() == '_mean':
                with edit(layer):
                    idx = layer.fields().indexFromName(field.name())
                    layer.renameAttribute(idx, new_field_name)
                
        input_layer = output_layer  

    # refactoring
    output_layer = working_directory + "\\" + vectors[layer_type][0] + ".shp"
    params = {
    'FIELDS_MAPPING' : field_list,
    'INPUT' : input_layer, 
    'OUTPUT' : output_layer
    }
    result = processing.run("native:refactorfields", params)
    print ("Refactored!")

    print("Finish vectorization (",layer_type,",",algorithm,")!")


def coverage_calculation():    

    # attribute list file
    attribute_file = attribute_update_file

    # directory to save output for each step
    # EXISTING CONTENTS WILL BE DELETED!
    working_directory = root_directory + r"\5_Coverage"

    # parameter to make buffer around extent for raster clipping
    extent_tolerance = 0.5

    # ----- Adjust Parameters Above -----

    print("\nStart coverage calculation...")

    # create folder
    if os.path.exists(working_directory):
        list( map( os.unlink, (os.path.join( working_directory,f) for f in os.listdir(working_directory)) ) )
    else:
        os.makedirs(working_directory)

    # import attributes
    att = []
    f_in = open(attribute_file,'r',encoding='latin1')
    f_in.readline()
    reader = csv.reader(f_in)
    for row in reader:
        att.append(row)
    f_in.close()

    # import sids countries
    sids = []
    header = ['attribute']
    f_in = open(sids_list_file,'r',encoding='latin1')
    f_in.readline()
    reader = csv.reader(f_in)
    for row in reader:
        sids.append(row)
        header.append(row[0])
    f_in.close()

    # set up overall output file
    f_out_all = open(working_directory+"\\coverage_temp.csv",'w', encoding='latin1',newline='')
    writer_all = csv.writer(f_out_all)
    writer_all.writerow(header)

    # iterate each attribute (raster data)
    for row in att:

        # init and parameters
        id = row[0]
        print("\nAttribute:",id)
        output_row = [id]
        input_raster = root_directory + "\\"+row[1]+"\\"+row[2]
        no_data = str(row[4])
        newpath = working_directory+"\\"+id
        if not os.path.exists(newpath):
            os.makedirs(newpath)
        
        # set up single-step output file
        f_out = open(working_directory+"\\"+id+"_coverage.csv",'w', encoding='latin1',newline='')
        writer = csv.writer(f_out)
        writer.writerow(header)
        
        # iterate each sids country
        for country in sids:
            
            # initialization
            sids_id = country[0]
            print("\nSIDS:",sids_id)
            newpath = working_directory+"\\"+id+"\\"+sids_id
            if not os.path.exists(newpath):
                os.makedirs(newpath)        
            
            # get extent with tolerance
            input_vector = root_directory+"\\"+country[1]+"\\"+country[2]
            vlayer = QgsVectorLayer(input_vector)
            features = vlayer.getFeatures()
            extent = ""
            for feature in features:
                extent_list = feature.attributes()[2].split(",")
                lon_min = float(extent_list[0])-extent_tolerance
                lon_max = float(extent_list[1])+extent_tolerance
                lat_min = float(extent_list[2])-extent_tolerance
                lat_max = float(extent_list[3])+extent_tolerance
                extent = str([lon_min,lon_max,lat_min,lat_max])[1:-1]
            
            print(input_raster)
            
            # 7-step processing

            # step 1: generally clip the raster by extent with a buffer zone
            print ("1-cliprasterbyextent",extent)
            result = processing.run("gdal:cliprasterbyextent", 
            {'INPUT':input_raster,'PROJWIN':extent,'OVERCRS':True,'NODATA':None,'OPTIONS':'','DATA_TYPE':0,'EXTRA':'','OUTPUT':newpath+'\\1-clip-extent.tif'})

            # step 2: convert the raster to binary value (only caring about coverage)
            print ("2-rastercalculator")
            if no_data == "":
                expr = '"1-clip-extent@1\"*0+1'
            else:
                expr = '((\"1-clip-extent@1\") / (\"1-clip-extent@1\" != '+no_data+'))*0+1'
            
            result = processing.run("qgis:rastercalculator", 
            {'EXPRESSION': expr,'LAYERS':[result['OUTPUT']],'CELLSIZE':0,'EXTENT':None,'CRS':None,'OUTPUT':newpath+'\\2-raster-calculator.tif'})

            # step 3: convert the raster cells with value 1 to vector shapes
            print ("3-polygonize")
            result = processing.run("gdal:polygonize", 
            {'INPUT':result['OUTPUT'],'BAND':1,'FIELD':'DN','EIGHT_CONNECTEDNESS':False,'EXTRA':'','OUTPUT':newpath+'\\3-polygonize.shp'})

            # step 4: fix the potential geometry errors
            print ("4-fixgeometries")
            result = processing.run("native:fixgeometries", 
            {'INPUT':result['OUTPUT'],'OUTPUT':newpath+'\\4-fix-geometries.shp'})

            # step 5: dissolve the fixed geometries to a huge polygon
            print ("5-dissolve")
            result = processing.run("native:dissolve", 
            {'INPUT':result['OUTPUT'],'FIELD':[],'OUTPUT':newpath+'\\5-dissolve.shp'})

            # step 6: intersect the dissolved huge polygon with sids countries' boundaries
            print ("6-intersection")
            result = processing.run("native:intersection", 
            {'INPUT':result['OUTPUT'],'OVERLAY':input_vector,'INPUT_FIELDS':[],'OVERLAY_FIELDS':[],'OVERLAY_FIELDS_PREFIX':'','OUTPUT':newpath+'\\6-intersection.shp'})

            # step 7: calculate the area for each intersected polygons
            print ("7-fieldcalculator")
            result = processing.run("native:fieldcalculator", 
            {'INPUT':result['OUTPUT'],'FIELD_NAME':'cover_area','FIELD_TYPE':0,'FIELD_LENGTH':9,'FIELD_PRECISION':3,'FORMULA':'area(transform($geometry, \'EPSG:4326\',\'ESRI:54034\'))/1000000','OUTPUT':newpath+'\\7-fieldcalculator.shp'})
            
            print ("Finish 7 steps. Calculating coverage...")
            
            # calculate coverage
            layer = QgsVectorLayer(result['OUTPUT'])
            features = layer.getFeatures()
            covered = False;
            for feature in features:
                attrs = feature.attributes()
                print(attrs)
                sids_area = float(attrs[2])
                cover_area = float(attrs[4])
                cover_per = cover_area/sids_area
                if cover_per > 1:
                    cover_per = 1 # correction
                output_row.append(cover_per)
                covered = True
            if not covered:
                output_row.append(0) # no coverage
            print ("Finish calculating coverage!")
        
        # single-step output
        writer.writerow(output_row)
        f_out.close()
        
        # overall output
        writer_all.writerow(output_row)    

    f_out_all.close()

    # deal with FJI_W, FJI_E, KIR_W, KIR_E (the two countries crossing 180-degree meridian)
    area=[881.1346,18082.3212,649.3666,362.7155]
    coverage_file = working_directory+"\\coverage_temp.csv"
    coverage_file_update = working_directory+"\\coverage_final.csv"

    f_in = open(coverage_file,'r',encoding='latin1')
    header = f_in.readline().strip().split(",")
    reader = csv.reader(f_in)
    
    # for testing purpose, change it to true
    testing = false    
    
    if not(testing):
        header.pop(18)
        header.pop(18)
        header.pop(23)
        header.pop(23)
        header.extend(["FJI","KIR"])    

    f_out = open(coverage_file_update,'w', encoding='latin1',newline='')
    writer = csv.writer(f_out)
    writer.writerow(header)

    for row in reader:
        if not(testing):
            FJI_W = float(row.pop(18))
            FJI_E = float(row.pop(18))
            KIR_W = float(row.pop(23))
            KIR_E = float(row.pop(23))
            row.append((FJI_W*area[0]+FJI_E*area[1])/(area[0]+area[1]))
            row.append((KIR_W*area[2]+KIR_E*area[3])/(area[2]+area[3]))        
        writer.writerow(row)
    f_in.close()
    f_out.close()

    print("Finish coverage calculation!")     


print("\nBEGIN PROCESSING!")

# Step 3
#standardization()

# Step 4
#vectorization("admin1","quantitative")
#vectorization("admin2","quantitative")
#vectorization("hex-10km","quantitative")
#vectorization("hex-5km","quantitative")
#vectorization("hex-1km","quantitative")
#vectorization("hex-10km-ocean","quantitative")
#vectorization("grid-10km","quantitative")
#vectorization("grid-5km","quantitative")
#vectorization("grid-1km","quantitative")
#vectorization("grid-10km-ocean","quantitative")

# Step 5
coverage_calculation()

print("\nAll DONE!")



### Below are the processing functions to be migrated out of QGIS GUI in the future
#processing.run("gdal:warpreproject", params)
#processing.run("gdal:rearrange_bands", params)
#processing.run("native:zonalstatisticsfb", params)
#processing.run("native:refactorfields", params)
#processing.run("gdal:cliprasterbyextent", params)
#processing.run("qgis:rastercalculator", params)
#processing.run("gdal:polygonize", params)
#processing.run("native:fixgeometries", params)
#processing.run("native:dissolve", params)
#processing.run("native:intersection", params)
#processing.run("native:fieldcalculator", params)
