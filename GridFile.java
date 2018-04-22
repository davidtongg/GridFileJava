import java.util.*;
import java.io.*;

public class GridFile {
    private long gridSize;
    private long pageSize;
    private long scaleSize;
    private long directorySize;
    private long bucketSize;
    private String gridName;
    private String scaleName;
    private String directoryName;
    private String bucketName;
    private MappedByteBuffer gridScale;
    private MappedByteBuffer gridDirectory;

    /*
        Creates a file with write permission
     */
    public void createFile(long size, String fname) {
        File f = null;

        try {
            f = new File(fname);
            f.createNewFile();
            f.setExecutable(false);
            f.setReadable(false);
            f.setWritable(true);
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("Error: createFile()");
        }
    }

    /*
        Creates a grid with specified grid size, page size, and name
     */
    public void createGrid(long size, long psize, String name) {
        MappedByteBuffer scaleRAF;
        MappedByteBuffer dirRAF;

        this.gridSize = size;
        this.pageSize = psize;
        this.scaleSize = (2 * this.gridSize + 1) * 8;
        this.directorySize = (this.gridSize * this.gridSize) * 5 * 8 + 8;
        this.bucketSize = (this.gridSize * this.gridSize) * this.pageSize;
        this.gridName = name;
        this.scaleName = name + "scale";
        this.directoryName = name + "directory";
        this.bucketName = name + "buckets";
        this.gridScale = null;
        this.gridDirectory = null;

        try {
            // scale file
            createFile(this.scaleSize, this.scaleName);
            scaleRAF = new RandomAccessFile(this.scaleName, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8);
            scaleRAF.write(size.getBytes());
            scaleRAF.close();

            // directory file
            createFile(this.directorySize, this.directoryName);
            dirRAF = new RandomAccessFile(this.directoryName, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8);
            dirRAF.seek(1); // does this do what is done below?
            dirRAF.close();

            createFile(this.bucketSize, this.bucketName);
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("Error: createGrid()");
        }
    }

    /*
        Maps grid scale file into memory
     */
    public void mapGridScale() {
        try {
            this.gridScale = new RandomAccessFile(this.scaleName, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.scaleSize);
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("Error: mapGridScale()")
        }
    }

    public void unmapGridScale() {
        try {
            this.gridScale.close();
            this.gridScale = null;
        } catch(IOException e) {
            //e.printStackTrace();
            System.out.println("Error: unmapGridScale()")
        }
    }

    /*
        Maps grid directory file into memory
     */
    public void mapGridDirectory() {
        try {
            this.gridDirectory = new RandomAccessFile(this.directoryName, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.directorySize);
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("Error: mapGridDirectory()");
        }
    }

    public void unmapGridDirectory() {
        try {
            this.gridDirectory.close();
            this.gridDirectory = null;
        } catch(IOException e) {
            //e.printStackTrace();
            System.out.println("Error: unmapGridDirectory()");
        }
    }

    /*
        Maps grid scale file and grid directory file into memory
     */
    public void loadGrid() {
        mapGridScale();
        mapGridDirectory();
    }

    /*
        Unmaps grid scale file and grid directory file from memory
     */
    public void unloadGrid() {
        unmapGridScale();
        unmapGridDirectory();
    }

    /*
        Fetches grid longitude and latitude for given coordinates from grid scale
     */
    public long[] getGridLocation(long x, long y) {
        try {
            this.gridScale.seek(1);
            long xint = this.gridScale.readLong(); // read at position 1
            this.gridScale.seek(1 + gridSize);
            long yint = this.gridScale.readLong(); // read at position 1 + gridSize

            long xpart = 2;
            long ypart = 2 + this.gridSize;
            long iter = 0;
            long[] lonlat = new long[2];

            this.gridScale.seek(xpart);
            while (iter < xint && x > this.gridScale.readLong()) { // determine longitude
                lonlat[0] = ++iter;
            }

            iter = 0;
            this.gridScale.seek(ypart);
            while (iter < yint && y > this.gridScale.readLong()) { // determine latitude
                lonlat[1] = ++iter;
            }
        } catch(IOException e) {
            //e.printStackTrace();
            System.out.println("Error: getGridLocation()");
        }

        return lonlat; // return "tuple", (longitude, latitude)
    }

    /*
        Inserts new grid partition in grid scale
     */
    public void insertGridPartition(int lon, long partition) {
        long ints = 0;
        long inta = 0; // used as offset
        long part = 0; // used as offset
        long iter = 0;
        long ipart = 0;

        try {
            if (lon == 1) { // longitude
                this.gridScale.seek(1);
                ints = this.gridScale.readLong(); // read at position 1
                inta = 1;
                part = 2;
            } else { // latitude
                this.gridScale.seek(1 + this.gridSize);
                ints = this.gridScale.readLong();
                inta = 1 + this.gridSize;
                part = 2 + this.gridSize;
            }

            if (ints >= this.gridSize - 1) {
                throw new OutOfMemoryError("Out of memory in insertGridPartition()");
            }

            this.gridScale.seek(part);
            while (iter < ints && partition > this.gridScale.readLong()) {
                iter++;
            }

            this.gridScale.seek(part + iter);
            if (iter < ints && this.gridScale.readLong() == partition) {
                throw new OutOfMemoryError("Out of memory in insertGridPartition()");
            }

            ipart = iter;

            this.gridScale.seek(part + ints);
            for (iter = ints; iter > ipart; iter--) {
                // original: part[iter] = part[iter - 1]
                this.gridScale.seek(part + iter - 1);
                long temp = this.gridScale.readLong(); // part[iter - 1]

                this.gridScale.seek(part + iter);
                this.gridScale.writeLong(temp); // part[iter]
            }

            this.gridScale.seek(part + ipart);
            this.girdScale.writeLong(partition);

            this.gridScale.seek(inta);
            long temp = this.gridScale.readLong() + 1;
            this.gridScale.seek(inta);
            this.gridScale.writeLong(temp);
        } catch(IOException e) {
            //e.printStackTrace();
            System.out.println("Error: insertGridPartition()");
        }
    }

    /*
        Fetches grid partitions for given coordinates
     */
    public long[] getGridPartitions(long lon, long lat) {
        try {
            this.gridScale.seek(1);
            long xint = this.gridScale.readLong();
            this.gridScale.seek(1 + gridSize);
            long yint = this.gridScale.readLong();
        } catch(IOException e) {
            //e.printStackTrace();
            System.out.println("Error: getGridPartitions()");
        }
        long xp = 0;
        long yp = 0;
        long xy[] = new long[2];

        if (lon > xint || lat > yint) {
            throw new IllegalArgumentException("Invalid argument in getGridPartitions()");
        }

        xp = (lon - 1 < 0) ? 0 : lon - 1;
        yp = (lat - 1 < 0) ? 0 : lat - 1;

        xy[0] = gridScale[2 + xp];
        xy[1] = gridScale[2 + this.gridSize + yp];

        return xy;
    }

    /*
        Returns offset for beginning of grid entry in grid directory
     */
    public long getGridEntry(long lon, long lat) {
        long offset = 1;
        try {
            this.gridScale.seek(1);
            long xint = this.gridScale.readLong();
            this.gridScale.seek(1 + this.gridSize);
            long yint = this.gridScale.readLong();
        } catch(IOException e) {
            //e.printStackTrace();
            System.out.println("Error: getGridEntry()");
        }

        if (lon > xint || lat > yint) {
            throw new IllegalArgumentException("Invalid argument in getGridEntry()");
        }

        offset += (lon * this.gridSize * 5 + lat * 5);

        return offset; // use gridScale.seek(offset) in calling method to determine location of gentry
    }

    /*
        Maps grid bucket into memory for given grid entry
     */
    public MappedByteBuffer mapGridBucket(long gentry) {
        try {
            this.gridScale.seek(gentry + 4);
            long baddr = this.gridScale.readLong();
            long boffset = baddr * this.pageSize;

            gbucket = new RandomAccessFile(bucketName, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, boffset, this.pageSize);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return gbucket;
    }

    /*
        Unmaps grid bucket from memory
     */
    public void unmapGridBucket(MappedByteBuffer gbucket) {
        gbucket.close();
        gbucket = null;
    }

    /*
        Appends x, y, record size and record at end of the bucket
     */
    public void appendBucketEntry(MappedByteBuffer gbucket, long x, long y, long rsize, Object record) {
        gbucket.seek(0);
        long nbytes = gbucket.readLong();
        long boffset = 16 + nbytes;
        gbucket.seek(boffset);

        gbucket.writeLong(x);
        gbucket.writeLong(y);
        gbucket.writeLong(rsize);

        //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memcpy' has no equivalent in Java:
        long pointer = gbucket.getFilePointer();
        gbucket.seek(pointer + 3);
        gbucket.write(record.getBytes());


        gbucket.seek(0);
        long temp = gbucket.readLong();
        gbucket.seek(0);
        gbucket.writeLong(temp + 24 + rsize);

        gbucket.seek(1);
        temp = gbucket.readLong();
        gbucket.seek(0);
        gbucket.writeLong(temp + 1);
    }

    /*STOP =====================================================================================================================*/

    public int getBucketEntry(long[][] bentry, long[] gbucket, long entry) {
        int error = 0;
        long nrecords = gbucket[1];
        String be = (String)gbucket + 16;
        long cbe = 0; // was pointer
        long rsize = 0;
        long iter = 0;

        if (entry >= nrecords) {
            error = -EINVAL;
            //goto clean;
        }

        for (iter = 0; iter < entry; iter++) {
            cbe = (long) be;
            rsize = cbe[2];
            be += (24 + rsize);
        }

        bentry = (long) be;
    }

    public int deleteBucketEntry(long[] gbucket, long entry) {
        int error = 0;
        long nbytes = gbucket[0];
        long nrecords = gbucket[1];
        long cbytes = nbytes;
        long cbe = 0; // was pointer
        long nbe = 0; // was pointer
        long rsize = 0;
        long iter = 0;

        if (entry >= nrecords) {
            error = -EINVAL;
            //goto clean;
        }

        for (iter = 0; iter < entry; iter++) {
            error = getBucketEntry(cbe, gbucket, iter);
            if (error < 0)
            {
                //goto clean;
            }

            cbytes -= (24 + cbe[2]);
        }

        error = getBucketEntry(cbe, gbucket, entry);
        if (error < 0) {
            //goto clean;
        }

        rsize = cbe[2];
        cbytes -= (24 + rsize);
        nbe = (long)((String)cbe + 24 + rsize);

        //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memmove' has no equivalent in Java:
        memmove(cbe, nbe, cbytes);

        gbucket[0] -= (24 + rsize);
        gbucket[1] -= 1;
    }

    public int insertGridRecord(long[] gentry, long x, long y, Object record, long rsize) {
        int error = 0;
        long nbytes = gentry[0];
        long nrecords = gentry[1];
        long sx = gentry[2];
        long sy = gentry[3];
        long esize = 24 + rsize;
        long capacity = this.pageSize - 16 - nbytes;
        long gbucket = 0; // was pointer

        if (esize > capacity) {
            error = -ENOMEM;
            //goto clean;
        }

        error = mapGridBucket(gentry, gbucket);
        if (error < 0) {
            //goto clean;
        }

        appendBucketEntry(gbucket, x, y, rsize, record);

        gentry[2] = sx + x;
        gentry[3] = sy + y;
        gentry[1] += 1;
        gentry[0] += (24 + rsize);

        unmapGridBucket(gbucket);
    }

    public int splitGrid(int vertical, long lon, long lat, long x, long y) {
        int error = 0;
        long ge = 0; // was pointer
        long xint = gridScale[1];
        long yint = gridScale[1 + gridSize];
        long sum = 0;
        long average = 0;
        long nrecords = 0;
        long xiter = 0;
        long yiter = 0;
        long cge = 0;
        long pge = 0;

        if (vertical != 0 && xint == gridSize - 1) {
            error = -ENOMEM;
            //goto clean;
        }

        if (vertical == 0 && yint == gridSize - 1) {
            error = -ENOMEM;
            //goto clean;
        }

        error = getGridEntry(lon, lat, ge);
        if (error < 0) {
            //goto clean;
        }

        nrecords = ge[1];

        if (vertical == 0) {
            sum = ge[3];
            average = (sum + y) / (nrecords + 1);

            error = insertGridPartition(vertical, average);
            if (error < 0) {
                //goto clean;
            }

            for (xiter = 0; xiter <= xint; xiter++) {
                for (yiter = yint + 1; yiter > lat; yiter--) {
                    error = getGridEntry(xiter, yiter, cge);
                    if (error < 0) {
                        //goto clean;
                    }

                    error = getGridEntry(xiter, yiter - 1, pge);
                    if (error < 0) {
                        //goto clean;
                    }

                    //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memcpy' has no equivalent in Java:
                    memcpy(cge, pge, 40);
                }
            }
        } else {
            sum = ge[2];
            average = (sum + x) / (nrecords + 1);

            error = insertGridPartition(vertical, average);
            if (error < 0) {
                //goto clean;
            }

            for (yiter = 0; yiter <= yint; yiter++) {
                for (xiter = xint + 1; xiter > lon; xiter--) {
                    error = getGridEntry(xiter, yiter, cge);
                    if (error < 0) {
                        //goto clean;
                    }

                    error = getGridEntry(xiter - 1, yiter, pge);
                    if (error < 0) {
                        //goto clean;
                    }

                    //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memcpy' has no equivalent in Java:
                    memcpy(cge, pge, 40);
                }
            }
        }
    }

    public int updateBucket(int direction, long slon, long slat, long dlon, long dlat, long baddr) {
        int error = 0;
        int compare = 0;
        long ge = 0; // was pointer
        long pge = 0; // was pointer

        error = getGridEntry(slon, slat, ge);
        if (error < 0) {
            //goto clean;
        }

        error = getGridEntry(dlon, dlat, pge);
        if (error < 0) {
            //goto clean;
        }

        //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memcmp' has no equivalent in Java:
        compare = memcmp(pge, ge, 40);

        if (compare != 0 && baddr == pge[4]) {
            //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memcpy' has no equivalent in Java:
            memcpy(pge, ge, 40);
            error = updatePairedBuckets(direction, dlon, dlat, baddr);
            if (error < 0) {
                //goto clean;
            }
        }
    }

    public int updatePairedBuckets(int direction, long lon, long lat, long baddr)
    {
        int error = 0;
        long xint = gridScale[1];
        long yint = gridScale[1 + gridSize];

        if (direction >= 0) {
            if (lon < xint) {
                error = updateBucket(direction, lon, lat, lon + 1, lat, baddr);
                if (error < 0) {
                    //goto clean;
                }
            }

            if (lat < yint) {
                error = updateBucket(direction, lon, lat, lon, lat + 1, baddr);
                if (error < 0) {
                    //goto clean;
                }
            }
        }

        if (direction <= 0) {
            if (lon > 0) {
                error = updateBucket(direction, lon, lat, lon - 1, lat, baddr);
                if (error < 0) {
                    //goto clean;
                }
            }

            if (lat > 0) {
                error = updateBucket(direction, lon, lat, lon, lat - 1, baddr);
                if (error < 0) {
                    //goto clean;
                }
            }
        }
    }

    public int splitBucket(int vertical, long slon, long slat, long dlon, long dlat) {
        int error = 0;
        long sge = 0; // was pointer
        long dge = 0; // was pointer
        long sb = 0; // was pointer
        long db = 0; // was pointer
        long xint = gridScale[1];
        long yint = gridScale[1 + gridSize];
        long avgx = 0;
        long avgy = 0;
        long iter = 0;
        long cbe = 0; // was pointer
        long ssx = 0;
        long ssy = 0;
        long dsx = 0;
        long dsy = 0;
        long sn = 0;
        long dn = 0;
        long sbytes = 0;
        long dbytes = 0;

        if (slon > xint || slat > yint || dlon > xint || dlat > yint) {
            error = -EINVAL;
            //goto clean;
        }

        error = getGridEntry(slon, slat, sge);
        if (error < 0) {
            //goto clean;
        }

        error = getGridEntry(dlon, dlat, dge);
        if (error < 0) {
            //goto clean;
        }

        dge[0] = 0;
        dge[1] = 0;
        dge[2] = 0;
        dge[3] = 0;
        dge[4] = gridDirectory[0];
        gridDirectory[0] += 1;

        error = getGridPartitions(avgx, avgy, dlon, dlat);
        if (error < 0) {
            //goto clean;
        }

        error = mapGridBucket(sge, sb);
        if (error < 0) {
            //goto clean;
        }

        error = mapGridBucket(dge, db);
        if (error < 0) {
            unmapGridBucket(sge);
            //goto clean;
        }

        sbytes = sge[0];
        sn = sge[1];
        ssx = sge[2];
        ssy = sge[3];

        dbytes = dge[0];
        dn = dge[1];
        dsx = dge[2];
        dsy = dge[3];

        if (vertical != 0) {
            while (iter < sn) {
                error = getBucketEntry(cbe, sb, iter);
                if (error < 0) {
                    //goto pclean;
                }

                if (cbe[0] > avgx) {
                    appendBucketEntry(db, cbe[0], cbe[1], cbe[2], cbe + 3);

                    sbytes -= (24 + cbe[2]);
                    dbytes += (24 + cbe[2]);
                    sn -= 1;
                    dn += 1;
                    ssx -= cbe[0];
                    ssy -= cbe[1];
                    dsx += cbe[0];
                    dsy += cbe[1];

                    error = deleteBucketEntry(sb, iter);
                    if (error < 0) {
                        //goto pclean;
                    }
                } else {
                    iter++;
                }
            }
        } else {
            while (iter < sn) {
                error = getBucketEntry(cbe, sb, iter);
                if (error < 0) {
                    //goto pclean;
                }

                if (cbe[1] > avgy) {
                    appendBucketEntry(db, cbe[0], cbe[1], cbe[2], cbe + 3);

                    sbytes -= (24 + cbe[2]);
                    dbytes += (24 + cbe[2]);
                    sn -= 1;
                    dn += 1;
                    ssx -= cbe[0];
                    ssy -= cbe[1];
                    dsx += cbe[0];
                    dsy += cbe[1];

                    error = deleteBucketEntry(sb, iter);
                    if (error < 0) {
                        //C++ TO JAVA CONVERTER TODO TASK: There are no gotos or labels in Java:
                        //goto pclean;
                    }
                } else {
                    iter++;
                }
            }
        }

        sge[0] = sbytes;
        sge[1] = sn;
        sge[2] = ssx;
        sge[3] = ssy;

        dge[0] = dbytes;
        dge[1] = dn;
        dge[2] = dsx;
        dge[3] = dsy;

        error = updatePairedBuckets(1, dlon, dlat, sge[4]);
        if (error < 0) {
            //C++ TO JAVA CONVERTER TODO TASK: There are no gotos or labels in Java:
            //goto pclean;
        }

        error = updatePairedBuckets(0, slon, slat, sge[4]);

        //pclean:
        //unmapGridBucket(sb);
        //unmapGridBucket(db);
    }

    public int checkPairedBucket(tangible.RefObject<Integer> isPaired, long slon, long slat, long dlon, long dlat) {
        int error = 0;
        long ge = 0; // was pointer
        long pge = 0; // was pointer

        error = getGridEntry(slon, slat, ge);
        if (error < 0) {
            //goto clean;
        }

        error = getGridEntry(dlon, dlat, pge);
        if (error < 0) {
            //goto clean;
        }

        if (ge[4] == pge[4]) {
            isPaired.argValue = 1;
        }
    }

    public int hasPairedBucket(int direction, tangible.RefObject<Integer> isPaired, tangible.RefObject<Integer> vertical, tangible.RefObject<Integer> forward, long lon, long lat) {
        int error = 0;
        isPaired.argValue = null;
        vertical.argValue = null;
        forward.argValue = null;
        long xint = gridScale[1];
        long yint = gridScale[1 + gridSize];

        if (direction <= 0) {
            if (lon > 0) {
                error = checkPairedBucket(isPaired.argValue, lon, lat, lon - 1, lat);
                if (error < 0) {
                    //goto clean;
                }

                if (isPaired.argValue) {
                    vertical.argValue = 1;
                    //goto clean;
                }
            }

            if (lat > 0) {
                error = checkPairedBucket(isPaired.argValue, lon, lat, lon, lat - 1);
                if (error < 0) {
                    //goto clean;
                }

                if (isPaired.argValue) {
                    //goto clean;
                }
            }
        }

        if (direction >= 0) {
            forward.argValue = 1;
            if (lon < xint) {
                error = checkPairedBucket(isPaired.argValue, lon, lat, lon + 1, lat);
                if (error < 0) {
                    //goto clean;
                }

                if (isPaired.argValue) {
                    vertical.argValue = 1;
                    //goto clean;
                }
            }

            if (lat < yint) {
                error = checkPairedBucket(isPaired.argValue, lon, lat, lon, lat + 1);
                if (error < 0) {
                    //goto clean;
                }

                if (isPaired.argValue) {
                    //goto clean;
                }
            }
        }
    }

    public int insertRecord(long x, long y, Object record, long rsize) {
        int error = 0;
        long lon = 0;
        long lat = 0;
        long ge = 0; // was pointer
        long nbytes = 0;
        long nrecords = 0;
        long capacity = 0;
        long esize = 24 + rsize;
        int isPaired = -1;
        int vertical = -1;
        int forward = -1;
        int split;
        long xint = gridScale[1];
        long yint = gridScale[1 + gridSize];
        split = xint == yint != 0 ? 1 : 0;

        getGridLocation(lon, lat, x, y);

        error = getGridEntry(lon, lat, ge);
        if (error < 0) {
            //goto clean;
        }

        nbytes = ge[0];
        nrecords = ge[1];
        capacity = this.pageSize - 16 - nbytes;

        if (esize <= capacity) {
            error = insertGridRecord(ge, x, y, record, rsize);
            if (error < 0) {
                //goto clean;
            }

            error = updatePairedBuckets(0, lon, lat, ge[4]);
            if (error < 0) {
                //goto clean;
            }
        } else {
            error = hasPairedBucket(0, isPaired, vertical, forward, lon, lat);
            if (error < 0) {
                //goto clean;
            }

            if (isPaired != 0) {
                if (vertical != 0) {
                    if (forward != 0) {
                        error = splitBucket(vertical, lon, lat, lon + 1, lat);
                    } else {
                        error = splitBucket(vertical, lon - 1, lat, lon, lat);
                    }
                }else {
                    if (forward != 0) {
                        error = splitBucket(vertical, lon, lat, lon, lat + 1);
                    } else {
                        error = splitBucket(vertical, lon, lat - 1, lon, lat);
                    }
                }

                if (error < 0) {
                    //goto clean;
                }
            } else {
                error = splitGrid(split, lon, lat, x, y);
                if (error < 0) {
                    //goto clean;
                }
            }

            error = insertRecord(x, y, record, rsize);
        }
    }

    public int findRecord(long x, long y, Object[] record) {
        int error = 0;
        int found = 0;
        long lon = 0;
        long lat = 0;
        long ge = 0; // was pointer
        long gb = 0; // was pointer
        long nrecords = 0;
        long iter = 0;
        long be = 0; // was pointer
        long bex = 0;
        long bey = 0;
        long rsize = 0;

        //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'malloc' has no equivalent in Java:
        record[0] = (Object)malloc(this.pageSize);
        if (record[0] == null) {
            error = -ENOMEM;
            //goto clean;
        }

        getGridLocation(lon, lat, x, y);

        error = getGridEntry(lon, lat, ge);
        if (error < 0) {
            //goto clean;
        }

        error = mapGridBucket(ge, gb);
        if (error < 0) {
            //goto clean;
        }

        nrecords = gb[1];

        for (iter = 0; iter < nrecords; iter++) {
            error = getBucketEntry(be, gb, iter);
            if (error < 0) {
                //goto pclean;
            }

            bex = be[0];
            bey = be[1];
            rsize = be[2];

            if (bex == x && bey == y) {
                found = 1;
                //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memcpy' has no equivalent in Java:
                memcpy(record[0], be + 3, rsize);
                break;
            }
        }
        /*
        pclean:
            unmapGridBucket(gb);

        clean:
            if (found == 0) {
                error = -EINVAL;
            }
            return error;
        */
    }

    public int deleteRecord(long x, long y) {
        int error = 0;
        int found = 0;
        long lon = 0;
        long lat = 0;
        long ge = 0; // was pointer
        long sx = 0;
        long sy = 0;
        long gb = 0; // was pointer
        long nrecords = 0;
        long iter = 0;
        long be = 0; // was pointer
        long bex = 0;
        long bey = 0;
        long rsize = 0;

        getGridLocation(lon, lat, x, y);

        error = getGridEntry(lon, lat, ge);
        if (error < 0) {
            //goto clean;
        }

        sx = ge[2];
        sy = ge[3];

        error = mapGridBucket(ge, gb);
        if (error < 0) {
            //goto clean;
        }

        nrecords = gb[1];

        for (iter = 0; iter < nrecords; iter++) {
            error = getBucketEntry(be, gb, iter);
            if (error < 0) {
                //goto pclean;
            }

            bex = be[0];
            bey = be[1];
            rsize = be[2];

            if (bex == x && bey == y) {
                found = 1;
                error = deleteBucketEntry(gb, iter);
                ge[0] -= (24 + rsize);
                ge[1] -= 1;
                ge[2] = sx - bex;
                ge[3] = sy - bey;
                break;
            }
        }

        if (found != 0) {
            error = updatePairedBuckets(0, lon, lat, ge[4]);
        }
        /*
        pclean:
            unmapGridBucket(gb);

        clean:
            if (found == 0) {
                error = -EINVAL;
            }

            return error;
        */
    }

    public int findRangeRecords(long x1, long y1, long x2, long y2, long dsize/*waspointer*/, Object[] records) {
        int error = 0;
        long lon1 = 0;
        long lat1 = 0;
        long lon2 = 0;
        long lat2 = 0;
        long iter = 0;
        long xiter = 0;
        long yiter = 0;
        long ge = 0; // was pointer
        long gb = 0; // was pointer
        long be = 0; // was pointer
        long nrecords = 0;
        long nr = 0;
        long bx = 0;
        long by = 0;
        long bs = 0;
        long rsize = 0;
        String rrecords = null;
        int isPaired = 0;
        int vertical = -1;
        int forward = -1;

        getGridLocation(lon1, lat1, x1, y1);
        getGridLocation(lon2, lat2, x2, y2);

        rsize = (lon2 - lon1 + 1) * (lat2 - lat1 + 1) * this.pageSize;
        //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'malloc' has no equivalent in Java:
        records[0] = (Object)malloc(rsize);
        if (records[0] == null) {
            error = -ENOMEM;
            //goto clean;
        }

        rrecords = (String)records + 8;

        for (xiter = lon1; xiter <= lon2; xiter++) {
            for (yiter = lat1; yiter <= lat2; yiter++) {
                error = getGridEntry(xiter, yiter, ge);
                if (error < 0) {
                    //goto clean;
                }

                error = hasPairedBucket(-1, isPaired, vertical, forward, xiter, yiter);
                if (error < 0) {
                    //goto clean;
                }

                if (isPaired != 0 && !((xiter == lon1 && vertical != 0) || (yiter == lat1 && vertical == 0))) {
                    continue;
                }

                error = mapGridBucket(ge, gb);
                if (error < 0) {
                    //goto clean;
                }

                nrecords = gb[1];

                for (iter = 0; iter < nrecords; iter++) {
                    error = getBucketEntry(be, gb, iter);
                    if (error < 0) {
                        unmapGridBucket(gb);
                        //goto clean;
                    }

                    bx = be[0];
                    by = be[1];
                    bs = be[2];

                    if (bx >= x1 != 0 && bx <= x2 != 0 && by >= y1 != 0 && by <= y2) {
                        nr += 1;
                        //C++ TO JAVA CONVERTER TODO TASK: The memory management function 'memcpy' has no equivalent in Java:
                        memcpy(rrecords, be, 24 + bs);
                        rrecords += (24 + bs);
                        dsize += (24 + bs); // was pointer
                    }
                }

                unmapGridBucket(gb);
            }
        }

        ((long) records[0])[0] = nr;
    }
}
