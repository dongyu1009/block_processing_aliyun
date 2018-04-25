# -*- encoding:utf-8 -*-
import osgeo.gdal as gdal
import numpy as np
import os
import copy
import math
import operator
import time
import shutil

def findmode(values):  
    bucket = {}  
    for value in values:  
        if bucket.has_key(value):  
            bucket[value] += 1  
        else:  
            bucket.setdefault(value,1)  
    bucket = sorted(bucket.iteritems(),key=operator.itemgetter(1),reverse=True)  
      
    modes  = []  
    for value in bucket:  
        if len(modes) == 0:  
            modes.append(value)  
        else:  
            temp = modes[len(modes)-1][1]  
            if temp == value[1]:  
                modes.append(value)  
            else:  
                break  
    return modes

	
def tonewclass(value):
    if value == 40 or value == 50:
        return 21
    if value == 60:
        return 22
    if value == 70:
        return 23
    if value == 90:
        return 24
    if value == 100:
        return 25
    return 25

if __name__ == '__main__':
    # config the parameters
    internel = 1000
    forestnumbers = [40, 50, 60, 70, 90, 100]
    
    # read the image
    shutil.copyfile("D:\\lucc1.tif","C:\\output\\lucc1.tif")

    ds_in1 = gdal.Open("C:\\output\\lucc1.tif", gdal.GA_Update) # GA_Update  GA_ReadOnly 
    im_width1 = ds_in1.RasterXSize
    im_height1 = ds_in1.RasterYSize
    # print im_width1, " ", im_height1
    # im_bands1 = ds_in1.RasterCount
    im_geotrans1 = ds_in1.GetGeoTransform()
    im_proj1 = ds_in1.GetProjection()
    nodatavalue = ds_in1.GetRasterBand(1).GetNoDataValue()
    
    ds_model = gdal.Open("D:\\lucc2.tif") # GA_Update  GA_ReadOnly 
    im_model_data = ds_model.ReadAsArray(0, 0, ds_model.RasterXSize, ds_model.RasterYSize)
    
    x_length = int(math.ceil(im_width1 / float(internel)))
    y_length = int(math.ceil(im_height1 / float(internel)))
    # specific operation
    for i in range(x_length):
        # print "S Processing : ", "%.2f%%" % (float(i) / x_length * 100)
        for j in range(y_length):
            # print "Processing : ", "%.2f%%" % (float(j) / y_length * 100 * (float(i) / x_length * 100))
            print "S Processing : ", "%.2f%%" % (float(i) / x_length * 100), "  I Processing : ", "%.2f%%" % (float(j) / y_length * 100)
            stime = time.time()
            x_min = i * internel
            y_min = j * internel
            x_max = min((i + 1) * internel, im_width1)
            y_max = min((j + 1) * internel, im_height1)
            im_data1 = ds_in1.ReadAsArray(x_min, y_min, x_max - x_min, y_max - y_min)
            # print "xmin", x_min, "y_min", y_min, "xmax", x_max, "ymax", y_max
            # print im_data1.shape
            for x in range(x_max - x_min):
                for y in range(y_max - y_min):
                    # print "x", x, " y", y 
                    value = im_data1[y][x]
                    if value == nodatavalue:
                        continue
                    if value / 10 == 2:
                        # modelvalue = ds_model.ReadAsArray(x_min + x, y_min + y, 1,1)[0][0]
                        modelvalue = im_model_data[x_min + x,y_min + y]
                        if modelvalue in forestnumbers:
                            value = tonewclass(modelvalue)
                        else:
                            radius = 1
                            flag = True
                            while(flag):
                                locationx = max(x_min + x - radius, 0)
                                locationx = min(locationx, im_width1 - radius * 2 - 1)
                                locationy = max(y_min + y - radius, 0)
                                locationy = min(locationy, im_height1 - radius * 2 - 1)
                                modelvalues = ds_model.ReadAsArray(locationx, locationy, radius * 2 + 1,radius * 2 + 1)
                                # modelvalues = im_model_data[locationx:(locationx + radius * 2 + 1),locationy:(locationy + radius * 2 + 1)]
                                modelvalues = np.where(modelvalues > 100, 0, modelvalues)
                                modelvalues = np.where(modelvalues < 40, 0, modelvalues)
                                mv_sum = modelvalues.sum()
                                modelvalues = modelvalues.reshape(modelvalues.size).tolist()
                                mode = findmode(filter(None, modelvalues))
                                if len(mode) == 0:
                                    radius = radius + 1
                                    continue
                                mode = mode[0][0]
                                if mv_sum != 0:
                                    flag = False
                                    value = tonewclass(mode)
                                    # value = mode
                                else:
                                    radius = radius + 1

                    im_data1[y][x] = value
            # res0 = copy.deepcopy(im_data1).astype(np.float32)

            # print im_data1.shape
            band = ds_in1.GetRasterBand(1)
            band.WriteArray(im_data1, x_min, y_min) # .astype(np.float32)
            etime = time.time()
            print "time consuming : %.2fs" % (etime - stime)
    
    # clean the variables
    del ds_in1
    del ds_model
    del im_data1
