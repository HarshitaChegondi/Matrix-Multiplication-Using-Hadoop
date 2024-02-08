/*  Full Name: Harshita Chegondi | Student ID : 1002115738
    Cloud Computing and Big Data Assignment 1
*/ 

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class MapperMatrixM implements Writable {
    public int i;
    public int j;
    public double value;

    MapperMatrixM() {}

    MapperMatrixM(int x, int y, double val) {
        i = x;
        j = y;
        value = val;
    }

    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
        value = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        out.writeDouble(value);
    }
}

class MapperMatrixN implements Writable {
    public int i;
    public int j;
    public double value;

    MapperMatrixN() {}

    MapperMatrixN(int x, int y, Double val) {
        i = x;
        j = y;
        value = val;
    }

    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
        value = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        out.writeDouble(value);
    }
}

class MapTags implements Writable {
    public short tag;
    public MapperMatrixM tagM;
    public MapperMatrixN tagN;

    MapTags() {}

    MapTags(MapperMatrixM m) {
        tag = 0;
        tagM = m;
    }

    MapTags(MapperMatrixN n) {
        tag = 1;
        tagN = n;
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(tag);
        if (tag == 0)
            tagM.write(out);
        else
            tagN.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        if (tag == 0) {
            tagM = new MapperMatrixM();
            tagM.readFields(in);
        } else {
            tagN = new MapperMatrixN();
            tagN.readFields(in);
        }
    }
}

class Pair implements WritableComparable<Pair> {
    int i;
    int j;

    public Pair() {}

    public Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    public int compareTo(Pair p) {
        if (this.i != p.i)
            return this.i - p.i;
        else
            return this.j - p.j;
    }

    public String toString() {
        return i + " " + j;
    }
}

public class Multi {
    public static class MapperM extends Mapper<Object, Text, IntWritable, MapTags> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            MapperMatrixM e = new MapperMatrixM(s.nextInt(), s.nextInt(), s.nextDouble());
            context.write(new IntWritable(e.j), new MapTags(e));
            s.close();
        }
    }

    public static class MapperN extends Mapper<Object, Text, IntWritable, MapTags> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            MapperMatrixN d = new MapperMatrixN(s.nextInt(), s.nextInt(), s.nextDouble());
            context.write(new IntWritable(d.i), new MapTags(d));
            s.close();
        }
    }

    public static class ResultReducerM extends Reducer<IntWritable,MapTags,Pair, DoubleWritable> {
        static Vector<MapperMatrixM> mM = new Vector<MapperMatrixM>();
        static Vector<MapperMatrixN> nN = new Vector<MapperMatrixN>();
        @Override
        public void reduce ( IntWritable key, Iterable<MapTags> values, Context context ) throws IOException, InterruptedException {
            mM.clear();
            nN.clear();
            for (MapTags v: values)
                if (v.tag == 0)
                    mM.add(v.tagM);
                else nN.add(v.tagN);
            for ( MapperMatrixM e: mM )
                for ( MapperMatrixN d: nN )
                    context.write(new Pair(e.i, d.j),new DoubleWritable(e.value*d.value));
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Text, Pair, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\\s+");
            Pair pair = new Pair(s.nextInt(), s.nextInt());
            DoubleWritable val = new DoubleWritable(s.nextDouble());
            context.write(pair, val);
            s.close();
        }
    }

    public static class ResultReducerN extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double m = 0;
            for (DoubleWritable d : values) {
                m += d.get();
            }
            context.write(key, new DoubleWritable(m));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "MatrixMultiplicationJob");
        job.setJarByClass(Multi.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapTags.class);

        job.setReducerClass(ResultReducerM.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperM.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperN.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "MatrixMultiplicationJob2");
        job2.setJarByClass(Multi.class);

        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(ResultReducerN.class);

        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
