from pygeoprocessing import geoprocessing as gp
import io
import os
import shapefile
from osgeo import gdal, ogr, gdalconst, osr
import json
import logging
import tempfile
import collections
import shutil
import numpy as np
import time



logger = logging.getLogger(__name__)

def mode(a, axis=0):
    scores = np.unique(np.ravel(a))       # get ALL unique values
    testshape = list(a.shape)
    testshape[axis] = 1
    oldmostfreq = np.zeros(testshape)
    oldcounts = np.zeros(testshape)
    in_dtype = a.dtype

    for score in scores:
        template = (a == score)
        counts = np.expand_dims(np.sum(template, axis),axis)
        mostfrequent = np.where(counts > oldcounts, score, oldmostfreq)
        oldcounts = np.maximum(counts, oldcounts)
        oldmostfreq = mostfrequent

    return np.asarray([mostfrequent], dtype=in_dtype).item()

def fetch_az_shapefile(blob_path=None, client_container=None, alternative_path=None):
    """
    Download a shapefile from an azure blob path
    :param blob_path: str, relativre path to a .shp file
    :param client_container, instance of instance of azure.storage.blob.ContainerClient
    :param alternative_path, str, a local dir, if provided the vector will be writen there as shp
           and subsequently read from there instead of going to AZ blob
           use for devel
    :return: a v2 GDAl/OGR Dataset with one layer representing the shapefile

    NB; the function will fetch/download the '.shp', '.shx', '.dbf', '.prj'
    files ands uses the pyshp (shapefile) lib to read the data because i could not find
    a  decent way in to read shapefiles from azure blob using GDAL/OGR directly.
    Instead a shapefile.Reader is created from Bytesio objects and this is exported
    to geoJSON using the __geo_interface__


    """
    blob_name = os.path.split(blob_path)[-1]

    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_shp_path = os.path.join(alternative_path, blob_name)
        if os.path.exists(dst_shp_path):
            logger.info(f'Reading {blob_path} from {alternative_path}')
            return gdal.OpenEx(dst_shp_path, gdalconst.OF_VECTOR|gdalconst.OF_READONLY)
            #return ogr.Open(dst_shp_path, gdal.OF_VECTOR|gdal.OF_READONLY)

    logger.info(f'Fetching {blob_path} from {client_container.container_name} container')

    root = os.path.splitext(blob_path)[0]
    args = dict()

    # make a sync container client

    for e in ('.shp', '.shx', '.dbf', '.prj' ):

        pth = f'{root}{e}'
        # fetch a stream
        rast_cfg_stream = client_container.download_blob(pth)
        # push the binary stream into RAM
        m = io.BytesIO()
        rast_cfg_stream.readinto(m)
        # need to position at the beginning
        m.seek(0)
        args[e[1:]] = m

    with shapefile.Reader(**args) as sr:
        #TODO think of a better way
        vds = gdal.OpenEx(json.dumps(sr.shapeRecords().__geo_interface__), gdalconst.OF_VECTOR|gdalconst.OF_READONLY)
        # save to shp in case this the alternative_path arg was provided
        if alternative_path is not None:
            assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
            logger.info(f'Creating {dst_shp_path}')
            dst_shp_path = os.path.join(alternative_path,blob_name)
            new_ds = gdal.VectorTranslate(destNameOrDestDS=dst_shp_path, srcDS=vds, layerName=blob_name)
            new_ds = None
            #close the in mem sources
            for k, v in args.items():
                v.close()

        return vds




def fetch_az_shapefile_direct(blob_path=None, client_container=None, alternative_path=None):
    """
    Download a shapefile from an azure blob path
    :param blob_path: str, relativre path to a .shp file
    :param client_container, instance of instance of azure.storage.blob.ContainerClient
    :param alternative_path, str, a local dir, if provided the vector will be writen there as shp
           and subsequently read from there instead of going to AZ blob
           use for devel
    :return: a v2 GDAl/OGR Dataset with one layer representing the shapefile

    NB; the function will fetch/download the '.shp', '.shx', '.dbf', '.prj'
    files and sore the stream into vsi virtual files


    """
    blob_name = os.path.split(blob_path)[-1]




    #gdal.SetConfigOption('SHAPE_RESTORE_SHX','YES')
    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_shp_path = os.path.join(alternative_path, blob_name)
        if os.path.exists(dst_shp_path):
            logger.info(f'Reading {blob_path} from {alternative_path}')
            return gdal.OpenEx(dst_shp_path, gdalconst.OF_VECTOR|gdalconst.OF_READONLY)
            #return ogr.Open(dst_shp_path, gdal.OF_VECTOR|gdal.OF_READONLY)

    logger.info(f'Fetching {blob_path} from {client_container.container_name} container')
    name = os.path.split(blob_path)[-1]
    root = os.path.splitext(blob_path)[0]
    rroot, ext = os.path.splitext(name)
    read_path = f'/vsimem/{rroot}{ext}'


    for e in ('.shp', '.shx', '.dbf'):
        vsi_pth = f'/vsimem/{rroot}{e}'
        remote_pth  = f'{root}{e}'
        strm  = client_container.download_blob(remote_pth)
        v = strm.readall()
        gdal.FileFromMemBuffer(vsi_pth, v)

    # fect projection
    prj_file = f'{root}.prj'
    sr = osr.SpatialReference()
    sr.ImportFromWkt(client_container.download_blob(prj_file).readall().decode('utf-8'))
    #vds = gdal.OpenEx(read_path, gdalconst.OF_VECTOR|gdalconst.OF_UPDATE )
    vds = gdal.OpenEx(read_path, 1)
    #gdal.Dataset.CopyLayer()
    l = vds.GetLayer(0)
    nl = vds.CopyLayer(l, 'a', dst_srswkt=sr.ExportToWkt() )


    vds.SetProjection(sr.ExportToWkt())
    #del vds

    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        logger.info(f'Creating {dst_shp_path}')
        dst_shp_path = os.path.join(alternative_path, blob_name)
        new_ds = gdal.VectorTranslate(destNameOrDestDS=dst_shp_path, srcDS=vds, layerName=blob_name)
        new_ds = None
    return vds



def zonal_statistics(
        base_raster_path_band, aggregate_vector_path,
        aggregate_layer_name=None, ignore_nodata=True,
        polygons_might_overlap=True, working_dir=None):
    """Collect stats on pixel values which lie within polygons.

    This function summarizes raster statistics including min, max,
    mean, and pixel count over the regions on the raster that are
    overlapped by the polygons in the vector layer. Statistics are calculated
    in two passes, where first polygons aggregate over pixels in the raster
    whose centers intersect with the polygon. In the second pass, any polygons
    that are not aggregated use their bounding box to intersect with the
    raster for overlap statistics.

    Note:
        There may be some degenerate cases where the bounding box vs. actual
        geometry intersection would be incorrect, but these are so unlikely as
        to be manually constructed. If you encounter one of these please email
        the description and dataset to richsharp@stanford.edu.

    Args:
        base_raster_path_band (tuple): a str/int tuple indicating the path to
            the base raster and the band index of that raster to analyze.
        aggregate_vector_path (string): a path to a polygon vector whose
            geometric features indicate the areas in
            ``base_raster_path_band`` to calculate zonal statistics.
        aggregate_layer_name (string): name of shapefile layer that will be
            used to aggregate results over.  If set to None, the first layer
            in the DataSource will be used as retrieved by ``.GetLayer()``.
            Note: it is normal and expected to set this field at None if the
            aggregating shapefile is a single layer as many shapefiles,
            including the common 'ESRI Shapefile', are.
        ignore_nodata: if true, then nodata pixels are not accounted for when
            calculating min, max, count, or mean.  However, the value of
            ``nodata_count`` will always be the number of nodata pixels
            aggregated under the polygon.
        polygons_might_overlap (boolean): if True the function calculates
            aggregation coverage close to optimally by rasterizing sets of
            polygons that don't overlap.  However, this step can be
            computationally expensive for cases where there are many polygons.
              this flag to False directs the function rasterize in one
            step.
        working_dir (string): If not None, indicates where temporary files
            should be created during this run.

    Return:
        nested dictionary indexed by aggregating feature id, and then by one
        of 'mode', 'mean'.  Example::

            {0: {
                 'mean': 0.5,
                 'mode': 1

                 }
            }

    Raises:
        ValueError
            if ``base_raster_path_band`` is incorrectly formatted.
        RuntimeError
            if the aggregate vector or layer cannot open.

    """
    if not gp._is_raster_path_band_formatted(base_raster_path_band):
        raise ValueError(
            "`base_raster_path_band` not formatted as expected.  Expects "
            "(path, band_index), received %s" % repr(base_raster_path_band))
    aggregate_vector = gdal.OpenEx(aggregate_vector_path, gdal.OF_VECTOR)
    if aggregate_vector is None:
        raise RuntimeError(
            "Could not open aggregate vector at %s" % aggregate_vector_path)
    if aggregate_layer_name is not None:
        aggregate_layer = aggregate_vector.GetLayerByName(
            aggregate_layer_name)
    else:
        aggregate_layer = aggregate_vector.GetLayer()
    if aggregate_layer is None:
        raise RuntimeError(
            "Could not open layer %s on %s" % (
                aggregate_layer_name, aggregate_vector_path))

    # create a new aggregate ID field to map base vector aggregate fields to
    # local ones that are guaranteed to be integers.
    local_aggregate_field_name = 'original_fid'
    rasterize_layer_args = {
        'options': [
            'ALL_TOUCHED=FALSE',
            'ATTRIBUTE=%s' % local_aggregate_field_name]
    }

    # clip base raster to aggregating vector intersection
    raster_info = gp.get_raster_info(base_raster_path_band[0])
    # -1 here because bands are 1 indexed
    raster_nodata = raster_info['nodata'][base_raster_path_band[1] - 1]
    temp_working_dir = tempfile.mkdtemp(dir=working_dir)
    clipped_raster_path = os.path.join(
        temp_working_dir, 'clipped_raster.tif')

    try:
        gp.align_and_resize_raster_stack(
            [base_raster_path_band[0]], [clipped_raster_path], ['near'],
            raster_info['pixel_size'], 'intersection',
            base_vector_path_list=[aggregate_vector_path],
            raster_align_index=0)
        clipped_raster = gdal.OpenEx(clipped_raster_path, gdal.OF_RASTER)
        clipped_band = clipped_raster.GetRasterBand(base_raster_path_band[1])
    except ValueError as e:
        if 'Bounding boxes do not intersect' in repr(e):
            logger.error(
                "aggregate vector %s does not intersect with the raster %s",
                aggregate_vector_path, base_raster_path_band)
            aggregate_stats = collections.defaultdict(
                lambda: {
                    # 'min': None, 'max': None, 'count': 0, 'nodata_count': 0,
                    # 'sum': 0.0,
                    'mean': 0.0, 'mode': 0,}
            )
            for feature in aggregate_layer:
                _ = aggregate_stats[feature.GetFID()]
            return dict(aggregate_stats)
        else:
            # this would be very unexpected to get here, but if it happened
            # and we didn't raise an exception, execution could get weird.
            raise

    # make a shapefile that non-overlapping layers can be added to
    driver = ogr.GetDriverByName('MEMORY')
    disjoint_vector = driver.CreateDataSource('disjoint_vector')
    spat_ref = aggregate_layer.GetSpatialRef()

    # Initialize these dictionaries to have the shapefile fields in the
    # original datasource even if we don't pick up a value later
    logger.info("build a lookup of aggregate field value to FID")

    aggregate_layer_fid_set = set(
        [agg_feat.GetFID() for agg_feat in aggregate_layer])
    agg_feat = None
    # Loop over each polygon and aggregate
    if polygons_might_overlap:
        logger.info("creating disjoint polygon set")
        disjoint_fid_sets = gp.calculate_disjoint_polygon_set(
            aggregate_vector_path, bounding_box=raster_info['bounding_box'])
    else:
        disjoint_fid_sets = [aggregate_layer_fid_set]

    agg_fid_raster_path = os.path.join(
        temp_working_dir, 'agg_fid.tif')

    agg_fid_nodata = -1
    gp.new_raster_from_base(
        clipped_raster_path, agg_fid_raster_path, gdal.GDT_Int32,
        [agg_fid_nodata])
    # fetch the block offsets before the raster is opened for writing
    agg_fid_offset_list = list(
        gp.iterblocks((agg_fid_raster_path, 1), offset_only=True))
    agg_fid_raster = gdal.OpenEx(
        agg_fid_raster_path, gdal.GA_Update | gdal.OF_RASTER)
    aggregate_stats = collections.defaultdict(lambda: { 'mean': 0.0, 'mode': 0,
        #'min': None, 'max': None, 'count': 0, 'nodata_count': 0, 'sum': 0.0
    })
    last_time = time.time()
    logger.info("processing %d disjoint polygon sets", len(disjoint_fid_sets))
    for set_index, disjoint_fid_set in enumerate(disjoint_fid_sets):
        last_time = gp._invoke_timed_callback(
            last_time, lambda: logger.info(
                "zonal stats approximately %.1f%% complete on %s",
                100.0 * float(set_index+1) / len(disjoint_fid_sets),
                os.path.basename(aggregate_vector_path)),
            gp._LOGGING_PERIOD)
        disjoint_layer = disjoint_vector.CreateLayer(
            'disjoint_vector', spat_ref, ogr.wkbPolygon)
        disjoint_layer.CreateField(
            ogr.FieldDefn(local_aggregate_field_name, ogr.OFTInteger))
        disjoint_layer_defn = disjoint_layer.GetLayerDefn()
        # add polygons to subset_layer
        disjoint_layer.StartTransaction()
        for index, feature_fid in enumerate(disjoint_fid_set):
            last_time = gp._invoke_timed_callback(
                last_time, lambda: logger.info(
                    "polygon set %d of %d approximately %.1f%% processed "
                    "on %s", set_index+1, len(disjoint_fid_sets),
                    100.0 * float(index+1) / len(disjoint_fid_set),
                    os.path.basename(aggregate_vector_path)),
                gp._LOGGING_PERIOD)
            agg_feat = aggregate_layer.GetFeature(feature_fid)
            agg_geom_ref = agg_feat.GetGeometryRef()
            disjoint_feat = ogr.Feature(disjoint_layer_defn)
            disjoint_feat.SetGeometry(agg_geom_ref.Clone())
            agg_geom_ref = None
            disjoint_feat.SetField(
                local_aggregate_field_name, feature_fid)
            disjoint_layer.CreateFeature(disjoint_feat)
        agg_feat = None
        disjoint_layer.CommitTransaction()

        logger.info(
            "disjoint polygon set %d of %d 100.0%% processed on %s",
            set_index+1, len(disjoint_fid_sets), os.path.basename(
                aggregate_vector_path))

        # nodata out the mask
        agg_fid_band = agg_fid_raster.GetRasterBand(1)
        agg_fid_band.Fill(agg_fid_nodata)
        logger.info(
            "rasterizing disjoint polygon set %d of %d %s", set_index+1,
            len(disjoint_fid_sets),
            os.path.basename(aggregate_vector_path))
        rasterize_callback = gp._make_logger_callback(
            "rasterizing polygon " + str(set_index+1) + " of " +
            str(len(disjoint_fid_set)) + " set %.1f%% complete %s")
        gdal.RasterizeLayer(
            agg_fid_raster, [1], disjoint_layer,
            callback=rasterize_callback, **rasterize_layer_args)
        agg_fid_raster.FlushCache()

        # Delete the features we just added to the subset_layer
        disjoint_layer = None
        disjoint_vector.DeleteLayer(0)

        # create a key array
        # and parallel min, max, count, and nodata count arrays
        logger.info(
            "summarizing rasterized disjoint polygon set %d of %d %s",
            set_index+1, len(disjoint_fid_sets),
            os.path.basename(aggregate_vector_path))
        for agg_fid_offset in agg_fid_offset_list:
            agg_fid_block = agg_fid_band.ReadAsArray(**agg_fid_offset)
            clipped_block = clipped_band.ReadAsArray(**agg_fid_offset)
            valid_mask = (agg_fid_block != agg_fid_nodata)
            valid_agg_fids = agg_fid_block[valid_mask]
            valid_clipped = clipped_block[valid_mask]
            for agg_fid in np.unique(valid_agg_fids):
                masked_clipped_block = valid_clipped[valid_agg_fids == agg_fid]
                # if raster_nodata is not None:
                #     clipped_nodata_mask = np.isclose(masked_clipped_block, raster_nodata)
                # else:
                #     clipped_nodata_mask = np.zeros(masked_clipped_block.shape, dtype=bool)
                # aggregate_stats[agg_fid]['nodata_count'] += (np.count_nonzero(clipped_nodata_mask))
                # if ignore_nodata:
                #     masked_clipped_block = (masked_clipped_block[~clipped_nodata_mask])
                # if masked_clipped_block.size == 0:
                #     continue
                #
                # if aggregate_stats[agg_fid]['min'] is None:
                #     aggregate_stats[agg_fid]['min'] = (
                #         masked_clipped_block[0])
                #     aggregate_stats[agg_fid]['max'] = (
                #         masked_clipped_block[0])
                #
                # aggregate_stats[agg_fid]['min'] = min(
                #     np.min(masked_clipped_block),
                #     aggregate_stats[agg_fid]['min'])
                # aggregate_stats[agg_fid]['max'] = max(
                #     np.max(masked_clipped_block),
                #     aggregate_stats[agg_fid]['max'])
                # aggregate_stats[agg_fid]['count'] += (
                #     masked_clipped_block.size)
                # aggregate_stats[agg_fid]['sum'] += np.sum(
                #     masked_clipped_block)
                aggregate_stats[agg_fid]['mean'] += np.mean(
                    (
                        np.mean(masked_clipped_block),
                        aggregate_stats[agg_fid]['mean']
                     )
                )
                aggregate_stats[agg_fid]['mode'] += mode(
                    (
                        np.asarray((mode(masked_clipped_block), aggregate_stats[agg_fid]['mode']),
                                   dtype=masked_clipped_block.dtype
                                   )
                     )
                )
    unset_fids = aggregate_layer_fid_set.difference(aggregate_stats)
    logger.debug(
        "unset_fids: %s of %s ", len(unset_fids),
        len(aggregate_layer_fid_set))
    clipped_gt = np.array(
        clipped_raster.GetGeoTransform(), dtype=np.float32)
    logger.debug("gt %s for %s", clipped_gt, base_raster_path_band)
    for unset_fid in unset_fids:
        unset_feat = aggregate_layer.GetFeature(unset_fid)
        unset_geom_ref = unset_feat.GetGeometryRef()
        if unset_geom_ref is None:
            logger.warn(
                f'no geometry in {aggregate_vector_path} FID: {unset_fid}')
            continue
        unset_geom_envelope = list(unset_geom_ref.GetEnvelope())
        unset_geom_ref = None
        unset_feat = None
        if clipped_gt[1] < 0:
            unset_geom_envelope[0], unset_geom_envelope[1] = (
                unset_geom_envelope[1], unset_geom_envelope[0])
        if clipped_gt[5] < 0:
            unset_geom_envelope[2], unset_geom_envelope[3] = (
                unset_geom_envelope[3], unset_geom_envelope[2])

        xoff = int((unset_geom_envelope[0] - clipped_gt[0]) / clipped_gt[1])
        yoff = int((unset_geom_envelope[2] - clipped_gt[3]) / clipped_gt[5])
        win_xsize = int(np.ceil(
            (unset_geom_envelope[1] - clipped_gt[0]) /
            clipped_gt[1])) - xoff
        win_ysize = int(np.ceil(
            (unset_geom_envelope[3] - clipped_gt[3]) /
            clipped_gt[5])) - yoff

        # clamp offset to the side of the raster if it's negative
        if xoff < 0:
            win_xsize += xoff
            xoff = 0
        if yoff < 0:
            win_ysize += yoff
            yoff = 0

        # clamp the window to the side of the raster if too big
        if xoff+win_xsize > clipped_band.XSize:
            win_xsize = clipped_band.XSize-xoff
        if yoff+win_ysize > clipped_band.YSize:
            win_ysize = clipped_band.YSize-yoff

        if win_xsize <= 0 or win_ysize <= 0:
            continue

        # here we consider the pixels that intersect with the geometry's
        # bounding box as being the proxy for the intersection with the
        # polygon itself. This is not a bad approximation since the case
        # that caused the polygon to be skipped in the first phase is that it
        # is as small as a pixel. There could be some degenerate cases that
        # make this estimation very wrong, but we do not know of any that
        # would come from natural data. If you do encounter such a dataset
        # please email the description and datset to richsharp@stanford.edu.
        unset_fid_block = clipped_band.ReadAsArray(
            xoff=xoff, yoff=yoff, win_xsize=win_xsize, win_ysize=win_ysize)

        if raster_nodata is not None:
            unset_fid_nodata_mask = np.isclose(
                unset_fid_block, raster_nodata)
        else:
            unset_fid_nodata_mask = np.zeros(
                unset_fid_block.shape, dtype=bool)

        valid_unset_fid_block = unset_fid_block[~unset_fid_nodata_mask]
        if valid_unset_fid_block.size == 0:
            # aggregate_stats[unset_fid]['min'] = 0.0
            # aggregate_stats[unset_fid]['max'] = 0.0
            # aggregate_stats[unset_fid]['sum'] = 0.0
            aggregate_stats[unset_fid]['mean'] = 0.0
            aggregate_stats[unset_fid]['mode'] = 0.0
        else:
            # aggregate_stats[unset_fid]['min'] = np.min(
            #     valid_unset_fid_block)
            # aggregate_stats[unset_fid]['max'] = np.max(
            #     valid_unset_fid_block)
            # aggregate_stats[unset_fid]['sum'] = np.sum(
            #     valid_unset_fid_block)
            aggregate_stats[unset_fid]['mean'] = np.mean(
                valid_unset_fid_block)
            aggregate_stats[unset_fid]['mode'] = mode(
                valid_unset_fid_block)

        # aggregate_stats[unset_fid]['count'] = valid_unset_fid_block.size
        # aggregate_stats[unset_fid]['nodata_count'] = np.count_nonzero(
        #     unset_fid_nodata_mask)

    unset_fids = aggregate_layer_fid_set.difference(aggregate_stats)
    logger.debug(
        "remaining unset_fids: %s of %s ", len(unset_fids),
        len(aggregate_layer_fid_set))
    # fill in the missing polygon fids in the aggregate stats by invoking the
    # accessor in the defaultdict
    for fid in unset_fids:
        _ = aggregate_stats[fid]

    logger.info(
        "all done processing polygon sets for %s", os.path.basename(
            aggregate_vector_path))

    # clean up temporary files
    spat_ref = None
    clipped_band = None
    clipped_raster = None
    agg_fid_raster = None
    disjoint_layer = None
    disjoint_vector = None
    aggregate_layer = None
    aggregate_vector = None

    shutil.rmtree(temp_working_dir)
    return dict(aggregate_stats)




def zonal_stats(raster_path_or_ds=None, vector_path_or_ds=None, band=None,
                ignore_nodata=True, polygons_might_overlap=True ):

    # wrap rast and vect into vsimem because the pygeoprocessing.zonal_statistics wants
    # paths not objects

    if 'Dataset' in raster_path_or_ds.__class__.__name__:
        _, raster_file_name = os.path.split(raster_path_or_ds.GetFileList()[0])
        raster_path = f'/vsimem/{raster_file_name}'
        gdal.Translate(raster_path,raster_path_or_ds)
    else:
        raster_path = raster_path_or_ds

    if 'DataSource' in vector_path_or_ds.__class__.__name__ or 'Dataset' in vector_path_or_ds.__class__.__name__  :
        #_, vector_file_name = os.path.split(vector_path_or_ds.GetFileList()[0])
        vector_path = f'/vsimem/{vector_path_or_ds.GetLayer(0).GetName()}'
        gdal.VectorTranslate(destNameOrDestDS=vector_path,srcDS=vector_path_or_ds)

    else:
        vector_path = vector_path_or_ds

    stat_res = zonal_statistics(base_raster_path_band=(raster_path, band),
                                aggregate_vector_path=vector_path,
                                ignore_nodata=ignore_nodata,
                                polygons_might_overlap=polygons_might_overlap
                                )
    return stat_res