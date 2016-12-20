import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RankValue extends Configured implements WritableComparable<RankValue>{
    private Text outlink;
    private DoubleWritable rank;
    boolean isOutlink;

    public RankValue(){}

    public RankValue(RankValue o){
        this.outlink = o.outlink;
        this.rank = o.rank;
        this.isOutlink = o.isOutlink;
    }

    public RankValue(Text outlink){
        this.outlink = outlink;
        this.rank = new DoubleWritable();
        this.isOutlink = true;
    }

    public RankValue(DoubleWritable rank){
        this.rank = rank;
        this.outlink = new Text();
        this.isOutlink = false;
    }

    @Override
    public int compareTo(RankValue o) {
        if (this.isOutlink && o.isOutlink){
            return -1 * this.outlink.compareTo(o.outlink);
        }

        return -1 * this.outlink.compareTo(o.outlink);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isOutlink);
        dataOutput.writeDouble(rank.get());
        dataOutput.writeUTF(outlink.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isOutlink = dataInput.readBoolean();
        rank = new DoubleWritable(dataInput.readDouble());
        outlink = new Text(dataInput.readUTF());
    }

    public String toString(){
        if (isOutlink){
            return outlink.toString();
        }
        else {
            return rank.toString();
        }
    }

    public boolean isOutlink() {
        return isOutlink;
    }

    public void setOutlink(boolean isOutlink) {
        this.isOutlink = isOutlink;
    }

    public Text getOutlink() {
        return outlink;
    }

    public void setOutlink(Text outlink) {
        this.outlink = outlink;
    }

    public DoubleWritable getRank() {
        return rank;
    }

    public void setRank(DoubleWritable rank) {
        this.rank = rank;
    }
}
