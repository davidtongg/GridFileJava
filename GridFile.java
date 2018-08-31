import java.util.*;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

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
    private final long LONGBYTES = Long.SIZE / 8;

    /*
        Creates a file with write permission
     */
    public void createFile(long size, String fname) {
        try {
            File f = new File(fname);
            f.createNewFile();
            f.setExecutable(false);
            f.setReadable(false);
            f.setWritable(true);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: createFile()");
        }
    }

    /*
        Creates a grid with specified grid size, page size, and name
     */
    public void createGrid(long size, long psize, String name) {
        MappedByteBuffer scaleMBB;
        MappedByteBuffer dirMBB;

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
            scaleMBB = new RandomAccessFile(this.scaleName, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8);
            scaleMBB.putLong(0, size);
            //scaleRAF.close();

            // directory file
            createFile(this.directorySize, this.directoryName);
            dirMBB = new RandomAccessFile(this.directoryName, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8);
            dirMBB.position(1 * LONGBYTES); // does this do what is done below?
            //dirRAF.close();

            createFile(this.bucketSize, this.bucketName);
        } catch (Exception e) {
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
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: mapGridScale()");
        }
    }

    public void unmapGridScale() {
        try {
            //this.gridScale.close();
            this.gridScale = null;
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: unmapGridScale()");
        }
    }

    /*
        Maps grid directory file into memory
     */
    public void mapGridDirectory() {
        try {
            this.gridDirectory = new RandomAccessFile(this.directoryName, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.directorySize);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: mapGridDirectory()");
        }
    }

    public void unmapGridDirectory() {
        try {
            //this.gridDirectory.close();
            this.gridDirectory = null;
        } catch (Exception e) {
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
        long[] lonlat = new long[2];
        try {
            long xint = this.gridScale.getLong(1 * LONGBYTES); // read at position 1
            long yint = this.gridScale.getLong((1 + this.gridSize) * LONGBYTES); // read at position 1 + gridSize

            long xpart = 2;
            long ypart = 2 + this.gridSize;
            long iter = 0;

            while (iter < xint && x > this.gridScale.getLong((xpart + iter) * LONGBYTES)) { // determine longitude
                lonlat[0] = ++iter;
            }

            iter = 0;
            while (iter < yint && y > this.gridScale.getLong((ypart + iter) * LONGBYTES)) { // determine latitude
                lonlat[1] = ++iter;
            }
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: getGridLocation()");
        }

        return lonlat; // return "tuple", (longitude, latitude)
    }

    /*
        Inserts new grid partition in grid scale
     */
    public void insertGridPartition(int lon, long partition) {
        long ints;
        long inta; // used as offset
        long part; // used as offset
        long iter = 0;
        long ipart;

        try {
            if (lon == 1) { // longitude
                ints = this.gridScale.getLong(1 * LONGBYTES); // read at position 1
                inta = 1;
                part = 2;
            } else { // latitude
                ints = this.gridScale.getLong((1 + this.gridSize) * LONGBYTES);
                inta = 1 + this.gridSize;
                part = 2 + this.gridSize;
            }

            if (ints >= this.gridSize - 1) {
                throw new OutOfMemoryError("Out of memory in insertGridPartition()");
            }

            while (iter < ints && partition > this.gridScale.getLong((part + iter) * LONGBYTES)) {
                iter++;
            }

            if (iter < ints && this.gridScale.getLong((part + iter) * LONGBYTES) == partition) {
                throw new OutOfMemoryError("Out of memory in insertGridPartition()");
            }

            ipart = iter;

            for (iter = ints; iter > ipart; iter--) {
                long prev = this.gridScale.getLong((part + iter - 1) * LONGBYTES);
                this.gridScale.putLong((part + iter) * LONGBYTES, prev);
            }

            this.gridScale.putLong((part + ipart) * LONGBYTES, partition);

            long temp = this.gridScale.getLong(inta * LONGBYTES);
            this.gridScale.putLong(inta * LONGBYTES, temp + 1);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: insertGridPartition()");
        }
    }

    /*
        Fetches grid partitions for given coordinates
     */
    public long[] getGridPartitions(long lon, long lat) {
        long xy[] = new long[2];
        try {
            long xint = this.gridScale.getLong(1 * LONGBYTES);
            long yint = this.gridScale.getLong((1 + this.gridSize) * LONGBYTES);
            long xp;
            long yp;

            if (lon > xint || lat > yint) {
                throw new IllegalArgumentException("Invalid argument in getGridPartitions()");
            }

            xp = (lon - 1 < 0) ? 0 : lon - 1;
            yp = (lat - 1 < 0) ? 0 : lat - 1;

            xy[0] = this.gridScale.getLong((2 + xp) * LONGBYTES);
            xy[1] = this.gridScale.getLong((2 + this.gridSize + yp) * LONGBYTES);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: getGridPartitions()");
        }

        return xy;
    }

    /*
        Returns offset for beginning of grid entry in grid directory
     */
    public long getGridEntry(long lon, long lat) {
        long offset = 1;
        long xint;
        long yint;
        try {
            xint = this.gridScale.getLong(1 * LONGBYTES);
            yint = this.gridScale.getLong((1 + this.gridSize) * LONGBYTES);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: getGridEntry()");
        }

        if (lon > xint || lat > yint) {
            throw new IllegalArgumentException("Invalid argument in getGridEntry()");
        }

        offset += (lon * this.gridSize * 5 + lat * 5);

        return offset;  // use gridScale.getLong(offset * LONGBYTES)
    }

    /*
        Maps grid bucket into memory for given grid entry
     */
    public MappedByteBuffer mapGridBucket(long gentry) {
        MappedByteBuffer gbucket;
        try {
            long baddr = this.gridScale.getLong((gentry + 4) * LONGBYTES);
            long boffset = baddr * this.pageSize;

            gbucket = new RandomAccessFile(bucketName, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, boffset * LONGBYTES, this.pageSize);
        } catch (Exception e) {
            //e.printStackTrace();
            unmapGridBucket(gbucket);
            System.out.println("Error: mapGridBucket()");
        }

        return gbucket;
    }

    /*
        Unmaps grid bucket from memory
     */
    public void unmapGridBucket(MappedByteBuffer gbucket) {
        try {
            //gbucket.close();
            gbucket = null;
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: unmapGridBucket()");
        }
    }

    /*
        Appends x, y, record size and record at end of the bucket
     */
    public void appendBucketEntry(MappedByteBuffer gbucket, long x, long y, long rsize, Object record) {
        try {
            long nbytes = gbucket.getLong(0);
            long boffset = 16 + nbytes;

            gbucket.putLong(boffset * LONGBYTES, x);
            gbucket.putLong((boffset + 1) * LONGBYTES, y);
            gbucket.putLong((boffset + 2) * LONGBYTES, rsize);
            gbucket.position((boffset + 3) * LONGBYTES);
            gbucket.put(record, 0, rsize); // memcpy

            long temp = gbucket.getLong(0);
            gbucket.putLong(0, temp + 24 + rsize);
            temp = gbucket.getLong(1 * LONGBYTES);
            gbucket.putLong(1 * LONGBYTES, temp + 1);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: appendBucketEntry()");
        }
    }

    /*
        Returns bucket entry offset from mapped grid bucket
     */
    public long getBucketEntry(MappedByteBuffer gbucket, long entry) {
        long be = 16 * LONGBYTES;
        try {
            long nrecords = gbucket.getLong(1 * LONGBYTES);
            //long cbe;
            long rsize;
            long iter;

            if (entry >= nrecords) {
                throw new IllegalArgumentException("Invalid argument in getBucketEntry()");
            }

            for (iter = 0; iter < entry; iter++) {
                rsize = gbucket.getLong((be + 2) * LONGBYTES);
                be += 24 + rsize;
            }
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: getBucketEntry()");
        }

        return be; // return offset of bucket entry
    }

    /*
        Deletes bucket entry from mapped grid bucket
     */
    public void deleteBucketEntry(MappedByteBuffer gbucket, long entry) {
        try {
            long nbytes = gbucket.getLong(0);
            long nrecords = gbucket.getLong(1 * LONGBYTES);
            long cbytes = nbytes;
            long cbe; // used as offset
            long nbe; // used as offset
            long rsize;
            long iter;

            if (entry >= nrecords) {
                throw new IllegalArgumentException("Invalid argument in deleteBucketEntry()");
            }

            for (iter = 0; iter < entry; iter++) {
                cbe = getBucketEntry(gbucket, iter);
                cbytes -= (24 + gbucket.getLong(cbe * LONGBYTES));
            }

            cbe = getBucketEntry(gbucket, entry);

            rsize = gbucket.getLong((2 + cbe) * LONGBYTES);
            cbytes -= (24 + rsize);
            nbe = cbe + 24 + rsize;

            // memmove(cbe, nbe, cbytes);
            byte[] record = new byte[cbytes];
            gbucket.position(nbe * LONGBYTES);
            gbucket.get(record, 0, cbytes);
            gbucket.position(cbe * LONGBYTES);
            gbucket.put(record, 0, cbytes);

            long temp = gbucket.getLong(0);
            gbucket.putLong(0, temp - (24 + rsize));
            temp = gbucket.getLong(1 * LONGBYTES);
            gbucket.putLong(1 * LONGBYTES, temp - 1);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: deleteBucketEntry()");
        }
    }

    /*
        Inserts new record into bucket, updating bucket and grid entry statistics
     */
    public void insertGridRecord(long gentry, long x, long y, Object record, long rsize) {
        MappedByteBuffer gbucket;
        try {
            long nbytes = this.gridDirectory.getLong(0);
            long nrecords = this.gridDirectory.getLong((1 + gentry) * LONGBYTES);
            long sx = this.gridDirectory.getLong((2 + gentry) * LONGBYTES);
            long sy = this.gridDirectory.getLong((3 + gentry) * LONGBYTES);
            long esize = 24 + rsize;
            long capacity = this.pageSize - 16 - nbytes;

            if (esize > capacity) {
                throw new OutOfMemoryError("Out of memory in insertGridRecord()");
            }

            gbucket = mapGridBucket(gentry);

            appendBucketEntry(gbucket, x, y, rsize, record);

            this.gridDirectory.putLong((2 + gentry) * LONGBYTES, sx + x);
            this.gridDirectory.putLong((3 + gentry) * LONGBYTES, sy + y);
            long temp = this.gridDirectory.getLong((1 + gentry) * LONGBYTES);
            this.gridDirectory.putLong((1 + gentry) * LONGBYTES, temp + 1);
            temp = this.gridDirectory.getLong(gentry * LONGBYTES);
            this.gridDirectory.putLong(gentry * LONGBYTES, temp + 24 + rsize);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: insertGridRecord()");
        }

        unmapGridBucket(gbucket);
    }

    /*
        Splits grid in one direction with new grid entries sharing buckets
     */
    public void splitGrid(int vertical, long lon, long lat, long x, long y) {
        try {
            long ge;
            long xint = this.gridScale.getLong(1 * LONGBYTES);
            long yint = this.gridScale.getLong((1 + gridSize) * LONGBYTES);
            long sum;
            long average;
            long nrecords;
            long xiter;
            long yiter;
            long cge = 0;
            long pge = 0;

            if (vertical == 1 && xint == gridSize - 1) {
                throw new OutOfMemoryError("Out of memory in splitGrid()");
            }

            if (vertical == 0 && yint == gridSize - 1) {
                throw new OutOfMemoryError("Out of memory in splitGrid()");
            }

            ge = getGridEntry(lon, lat);
            nrecords = this.gridDirectory.getLong((ge + 1) * LONGBYTES);

            if (vertical == 0) {
                sum = this.gridDirectory.getLong((ge + 3) * LONGBYTES);
                average = (sum + y) / (nrecords + 1);

                insertGridPartition(vertical, average);

                for (xiter = 0; xiter <= xint; xiter++) {
                    for (yiter = yint + 1; yiter > lat; yiter--) {
                        cge = getGridEntry(xiter, yiter);
                        pge = getGridEntry(xiter, yiter - 1);

                        byte[] temp = new byte[40];
                        this.gridDirectory.position(pge * LONGBYTES);
                        this.gridDirectory.get(temp, 0, 40);
                        this.gridDirectory.position(cge * LONGBYTES);
                        this.gridDirectory.put(temp, 0, 40); // memcpy
                    }
                }
            } else {
                sum = this.gridDirectory.getLong((ge + 2) * LONGBYTES);
                average = (sum + x) / (nrecords + 1);

                insertGridPartition(vertical, average);

                for (yiter = 0; yiter <= yint; yiter++) {
                    for (xiter = xint + 1; xiter > lon; xiter--) {
                        cge = getGridEntry(xiter, yiter);
                        pge = getGridEntry(xiter - 1, yiter);

                        byte[] temp = new byte[40];
                        this.gridDirectory.position(pge * LONGBYTES);
                        this.gridDirectory.get(temp, 0, 40);
                        this.gridDirectory.position(cge * LONGBYTES);
                        this.gridDirectory.put(temp, 0, 40); // memcpy
                    }
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: splitGrid()");
        }
    }

    /*
        Updates destination entry from soure entry if sharing buckets
     */
    public void updateBucket(int direction, long slon, long slat, long dlon, long dlat, long baddr) {
        boolean compare;
        long ge = 0; // was pointer
        long pge = 0; // was pointer

        ge = getGridEntry(slon, slat);
        pge = getGridEntry(dlon, dlat);

        byte[] geTemp = new byte[40];
        byte[] pgeTemp = new byte[40];

        try {
            this.gridDirectory.position(ge * LONGBYTES);
            this.gridDirectory.get(geTemp, 0, 40);
            this.gridDirectory.position(pge * LONGBYTES);
            this.gridDirectory.get(pgeTemp, 0, 40);
            compare = geTemp.equals(pgeTemp);

            if (!compare && baddr == this.gridDirectory.getLong((4 + pge) * LONGBYTES)) {
                this.gridDirectory.position(pge * LONGBYTES);
                this.gridDirectory.put(geTemp, 0, 40);
                updatePairedBuckets(direction, dlon, dlat, baddr);
            }
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: updateBucket()");
        }
    }

    /*
        Updates paired buckets in given direction with statistics and address
     */
    public void updatePairedBuckets(int direction, long lon, long lat, long baddr)
    {
        int error = 0;
        long xint;
        long yint;

        try {
            xint = this.gridScale.getLong(1 * LONGBYTES);
            yint = this.gridScale.getLong((1 + gridSize) * LONGBYTES);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error: updatePairedBuckets()");
        }

        if (direction >= 0) {
            if (lon < xint) {
                updateBucket(direction, lon, lat, lon + 1, lat, baddr);
            }
            if (lat < yint) {
                updateBucket(direction, lon, lat, lon, lat + 1, baddr);
            }
        }

        if (direction <= 0) {
            if (lon > 0) {
                updateBucket(direction, lon, lat, lon - 1, lat, baddr);
            }
            if (lat > 0) {
                updateBucket(direction, lon, lat, lon, lat - 1, baddr);
            }
        }
    }

    /*
        Divides entries of paired buckets into individual buckets
     */
    public void splitBucket(int vertical, long slon, long slat, long dlon, long dlat) {
        long sge;
        long dge;
        MappedByteBuffer sb;
        MappedByteBuffer db;
        long xint = this.gridScale.getLong(1 * LONGBYTES);
        long yint = this.gridScale.getLong((1 + this.gridSize) * LONGBYTES);
        long avgx;
        long avgy;
        long iter = 0;
        long cbe;
        long ssx;
        long ssy;
        long dsx;
        long dsy;
        long sn;
        long dn;
        long sbytes;
        long dbytes;

        if (slon > xint || slat > yint || dlon > xint || dlat > yint) {
            throw new IllegalArgumentException("Invalid argument in splitBucket()");
        }

        sge = getGridEntry(slon, slat);
        dge = getGridEntry(dlon, dlat);

        this.gridDirectory.putLong(dge * LONGBYTES, 0);
        this.gridDirectory.putLong((1 + dge) * LONGBYTES, 0);
        this.gridDirectory.putLong((2 + dge) * LONGBYTES, 0);
        this.gridDirectory.putLong((3 + dge) * LONGBYTES, 0);

        long temp = this.gridDirectory.getLong(0);
        this.gridDirectory.putLong((4 + dge) * LONGBYTES, temp);
        this.gridDirectory.putLong(0, temp + 1);

        long[] xy = getGridPartitions(dlon, dlat);
        avgx = xy[0];
        avgy = xy[1];

        sb = mapGridBucket(sge);
        try {
            db = mapGridBucket(dge);
        } catch (Exception e) {
            unmapGridBucket(sge); // ????????????????????????????????
        }

        sbytes = this.gridDirectory.getLong(sge * LONGBYTES);
        sn = this.gridDirectory.getLong((1 + sge) * LONGBYTES);
        ssx = this.gridDirectory.getLong((2 + sge) * LONGBYTES);
        ssy = this.gridDirectory.getLong((3 + sge) * LONGBYTES);

        dbytes = this.gridDirectory.getLong(dge * LONGBYTES);
        dn = this.gridDirectory.getLong((1 + dge) * LONGBYTES);
        dsx = this.gridDirectory.getLong((2 + dge) * LONGBYTES);
        dsy = this.gridDirectory.getLong((3 + dge) * LONGBYTES);

        try {
            if (vertical == 1) {
                while (iter < sn) {
                    cbe = getBucketEntry(sb, iter);

                    if (sb.getLong(cbe * LONGBYTES) > avgx) {
                        long zero = this.gridDirectory.getLong(cbe * LONGBYTES);
                        long one = this.gridDirectory.getLong((1 + cbe) * LONGBYTES);
                        long two = this.gridDirectory.getLong((2 + cbe) * LONGBYTES);
                        appendBucketEntry(db, zero, one, two, cbe + 3); // how to pass in a record?

                        sbytes -= (24 + two);
                        dbytes += (24 + two);
                        sn -= 1;
                        dn += 1;
                        ssx -= zero;
                        ssy -= one;
                        dsx += zero;
                        dsy += one;

                        deleteBucketEntry(sb, iter);
                    } else {
                        iter++;
                    }
                }
            } else {
                while (iter < sn) {
                    cbe = getBucketEntry(sb, iter);

                    if (sb.getLong((1 + cbe) * LONGBYTES) > avgy) {
                        long zero = this.gridDirectory.getLong(cbe * LONGBYTES);
                        long one = this.gridDirectory.getLong((1 + cbe) * LONGBYTES);
                        long two = this.gridDirectory.getLong((2 + cbe) * LONGBYTES);
                        appendBucketEntry(db, zero, one, two, cbe + 3); // how to pass in a record?

                        sbytes -= (24 + two);
                        dbytes += (24 + two);
                        sn -= 1;
                        dn += 1;
                        ssx -= zero;
                        ssy -= one;
                        dsx += zero;
                        dsy += one;

                        deleteBucketEntry(sb, iter);
                    } else {
                        iter++;
                    }
                }
            }
        } catch (Exception e) {
            unmapGridBucket(sb);
            unmapGridBucket(db);
            System.out.println("Error: splitBucket(), Unmapped sb and db");
        }

        this.gridDirectory.putLong(sge * LONGBYTES, sbytes);
        this.gridDirectory.putLong((1 + sge) * LONGBYTES, sn);
        this.gridDirectory.putLong((2 + sge) * LONGBYTES, ssx);
        this.gridDirectory.putLong((3 + sge) * LONGBYTES, ssy);

        this.gridDirectory.putLong(dge * LONGBYTES, dbytes);
        this.gridDirectory.putLong((1 + dge) * LONGBYTES, dn);
        this.gridDirectory.putLong((2 + dge) * LONGBYTES, dsx);
        this.gridDirectory.putLong((3 + dge) * LONGBYTES, dsy);

        try {
            long four = this.gridDirectory.getLong((4 + sge) * LONGBYTES);
            updatePairedBuckets(1, dlon, dlat, four);
            updatePairedBuckets(0, slon, slat, four);
        } catch (Exception e) {
            unmapGridBucket(sb);
            unmapGridBucket(db);
            System.out.println("Error: splitBucket(), Unmapped sb and db");
        }
    }

    /*
        Checks if entries share buckets
     */
    public boolean checkPairedBucket(long slon, long slat, long dlon, long dlat) {
        long ge;
        long pge;

        ge = getGridEntry(slon, slat);
        pge = getGridEntry(dlon, dlat);

        if (this.gridDirectory.getLong((4 + ge) * LONGBYTES) ==
                this.gridDirectory.getLong(4 + pge) * LONGBYTES) {
            return true;
        }
    }

    /*
        Checks if paired bucket for given bucket exists
        Returns boolean array [isPaired, vertical, forward]
     */
    public boolean[] hasPairedBucket(int direction, long lon, long lat) {
        long xint = this.gridScale.getLong(1 * LONGBYTES);
        long yint = this.gridScale.getLong((1 + gridSize) * LONGBYTES);
        boolean[] ret = new boolean[3];

        if (direction <= 0) {
            if (lon > 0) {
                if (checkPairedBucket(lon, lat, lon - 1, lat)) {
                    ret[0] = true;
                    ret[1] = true;
                    return ret;
                }
            }
            if (lat > 0) {
                if (checkPairedBucket(lon, lat, lon, lat - 1)) {
                    ret[0] = true;
                    return ret;
                }
            }
        }

        if (direction >= 0) {
            ret[2] = true;
            if (lon < xint) {
                if (checkPairedBucket(lon, lat, lon + 1, lat)) {
                    ret[0] = true;
                    ret[1] = true;
                    return ret;
                }
            }
            if (lat < yint) {
                if (checkPairedBucket( lon, lat, lon, lat + 1)) {
                    ret[0] = true;
                    return ret;
                }
            }
        }
    }

    /*
        Inserts new record in the grid
     */
    public void insertRecord(long x, long y, Object record, long rsize) {
        long ge; // was pointer
        long nbytes;
        long nrecords;
        long capacity;
        long esize = 24 + rsize;
        boolean isPaired;
        boolean vertical;
        boolean forward;
        int split;
        long xint = this.gridScale.getLong(1 * LONGBYTES);
        long yint = this.gridScale((1 + gridSize) * LONGBYTES);
        split = xint == yint != 0 ? 1 : 0;

        long[] lonlat = getGridLocation(x, y);

        ge = getGridEntry(lonlat[0], lonlat[1]);

        nbytes = this.gridDirectory.getLong(ge * LONGBYTES);
        nrecords = this.gridDirectory.getLong((1 + ge) * LONGBYTES);
        capacity = this.pageSize - 16 - nbytes;

        if (esize <= capacity) {
            insertGridRecord(ge, x, y, record, rsize);
            updatePairedBuckets(0, lon, lat, ge[4]);
        } else {
            boolean temp[] = hasPairedBucket(0, isPaired, vertical, forward, lon, lat);
            isPaired = temp[0];
            vertical = temp[1];
            forward = temp[2];

            if (isPaired) {
                if (vertical) {
                    if (forward) {
                        splitBucket(vertical, lon, lat, lon + 1, lat);
                    } else {
                        splitBucket(vertical, lon - 1, lat, lon, lat);
                    }
                }else {
                    if (forward != 0) {
                        splitBucket(vertical, lon, lat, lon, lat + 1);
                    } else {
                        splitBucket(vertical, lon, lat - 1, lon, lat);
                    }
                }
            } else {
                splitGrid(split, lon, lat, x, y);
            }

            insertRecord(x, y, record, rsize);
        }
    }

    /*
        Returns record for given coordinates
     */
    public Object findRecord(long x, long y) {
        int found = 0;
        long lon;
        long lat;
        long ge;
        MappedByteBuffer gb = 0;
        long nrecords;
        long iter = 0;
        long be;
        long bex;
        long bey;
        long rsize;

        long[] lonlat = getGridLocation(x, y);
        lon = lonlat[0];
        lat = lonlat[1];

        try {
            ge = getGridEntry(lon, lat);
            gb = mapGridBucket(ge);
        } catch (Exception e) {
            //e.printStackTrace();
            throw new IllegalArgumentException("Invalid argument in findRecord()");
        }

        try {
            nrecords = gb.getLong(1 * LONGBYTES);

            for (iter = 0; iter < nrecords; iter++) {
                be = getBucketEntry(gb, iter);

                bex = gb.getLong(be * LONGBYTES);
                bey = gb.getLong((1 + be) * LONGBYTES);
                rsize = gb.getLong((2 + be) * LONGBYTES);

                byte[] record = new byte[rsize];

                if (bex == x && bey == y) {
                    found = 1;
                    gb.position((be + 3) * LONGBYTES);
                    gb.get(record, 0, rsize);
                    break;
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            unmapGridBucket(gb);
            System.out.println("Error: findRecord()");
        }

        unmapGridBucket(gb);
        if (found == 0) {
            throw new IllegalArgumentException("Invalid argument in findRecord()");
        }

        ByteArrayInputStream in = new ByteArrayInputStream(record);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    /*
        Deletes record for given coordinates
     */
    public void deleteRecord(long x, long y) {
        int found = 0;
        long lon;
        long lat;
        long ge;
        long sx;
        long sy;
        MappedByteBuffer gb;
        long nrecords;
        long iter = 0;
        long be;
        long bex;
        long bey;
        long rsize;

        long[] lonlat = getGridLocation(x, y);
        lon = lonlat[0];
        lat = lonlat[1];

        try {
            ge = getGridEntry(lon, lat);

            sx = this.gridDirectory.getLong((2 + ge) * LONGBYTES);
            sy = this.gridDirectory.getLong((3 + ge) * LONGBYTES);

            gb = mapGridBucket(ge);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument in deleteRecord()");
        }

        nrecords = gb[1];

        try {
            for (iter = 0; iter < nrecords; iter++) {
                be = getBucketEntry(gb, iter);

                bex = gb.getLong(be * LONGBYTES);
                bey = gb.getLong((1 + be) * LONGBYTES);
                rsize = gb.getLong((2 + be) * LONGBYTES);

                if (bex == x && bey == y) {
                    found = 1;
                    deleteBucketEntry(gb, iter);
                    long temp = this.gridDirectory.getLong(ge * LONGBYTES);
                    this.gridDirectory.putLong(ge * LONGBYTES, temp - (24 + rsize));
                    temp = this.gridDirectory.getLong((1 + ge) * LONGBYTES);
                    this.gridDirectory.putLong((1 + ge) * LONGBYTES, temp - 1);
                    temp = this.gridDirectory.getLong((2 + ge) * LONGBYTES);
                    this.gridDirectory.putLong((2 + ge) * LONGBYTES, sx - bex);
                    temp = this.gridDirectory.getLong((3 + ge) * LONGBYTES);
                    this.gridDirectory.putLong((3 + ge) * LONGBYTES, sy - bey);
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Error: deleteRecord()");
            unmapGridBucket(gb);
        }

        if (found != 0) {
            long temp = this.gridDirectory.getLong((4 + ge) * LONGBYTES);
            updatePairedBuckets(0, lon, lat, temp);
        }
        unmapGridBucket(gb);

        if (found == 0) {
            throw new IllegalArgumentException("Invalid argument in deleteRecord()");
        }
    }

    /*
        Retrieves record within specified coordinate range
     */
    public byte[] findRangeRecords(long x1, long y1, long x2, long y2, long dsize) {
        long lon1;
        long lat1;
        long lon2;
        long lat2;
        long iter = 0;
        long xiter = 0;
        long yiter = 0;
        long ge;
        MappedByteBuffer gb;
        long be;
        long nrecords;
        long nr;
        long bx;
        long by;
        long bs;
        long rsize;
        long rrecords = 8;
        boolean isPaired;
        boolean vertical;
        boolean forward;
        byte[] records;

        long[] lonlat1 = getGridLocation(x1, y1);
        lon1 = lonlat1[0];
        lat1 = lonlat1[1];
        long[] lonlat2 = getGridLocation(x2, y2);
        lon2 = lonlat2[0];
        lat2 = lonlat2[1];

        //save
        rsize = (lon2 - lon1 + 1) * (lat2 - lat1 + 1) * this.pageSize;
        records = new byte[rsize];

        try {
            for (xiter = lon1; xiter <= lon2; xiter++) {
                for (yiter = lat1; yiter <= lat2; yiter++) {
                    ge = getGridEntry(xiter, yiter);
                    boolean[] temp = hasPairedBucket(-1, isPaired, vertical, forward, xiter, yiter);
                    isPaired = temp[0];
                    vertical = temp[1];
                    forward = temp[2];

                    if (isPaired != 0 && !((xiter == lon1 && vertical != 0) || (yiter == lat1 && vertical == 0))) {
                        continue;
                    }

                    gb = mapGridBucket(ge);
                    nrecords = gb.getLong(1 * LONGBYTES);

                    for (iter = 0; iter < nrecords; iter++) {
                        be = getBucketEntry(gb, iter);
                        if (error < 0) {
                            unmapGridBucket(gb);
                        }

                        bx = this.gridDirectory.getLong(be * LONGBYTES);
                        by = this.gridDirectory.getLong((1 + be) * LONGBYTES);
                        bs = this.gridDirectory.getLong((2 + be) * LONGBYTES);

                        if (bx >= x1 != 0 && bx <= x2 != 0 && by >= y1 != 0 && by <= y2) {
                            nr += 1;
                            this.gridDirectory.position(be);
                            this.gridDirectory.get(records, rrecords, 24 + bs);
                            rrecords += (24 + bs);
                            dsize += (24 + bs);
                        }
                    }
                    unmapGridBucket(gb);
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Error; getRangeRecords()");
        }

        records.putLong(0, nr);
        return records;
    }
}
