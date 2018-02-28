package nca;

import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public abstract class EntryJoinMapperBase extends DataJoinMapperBase {

    protected static final Text LEFT_TAG = new Text(NCAConfig.LEFT_TAG);
    protected static final Text RIGHT_TAG = new Text(NCAConfig.RIGHT_TAG);

    protected String leftFile = null;
    protected String rightFile = null;

    @Override
    public void configure(JobConf job) {
        this.leftFile = job.get(NCAConfig.LEFT_FILE);
        this.rightFile = job.get(NCAConfig.RIGHT_FILE);
        super.configure(job);
    }

    @Override
    protected Text generateInputTag(String inputFile) {
        boolean left = inputFile.contains(leftFile);
        boolean right = inputFile.contains(rightFile);
        if (left && !right)
            return LEFT_TAG;
        else if (right && !left)
            return RIGHT_TAG;
        else if (left && right) {
            if (leftFile.length() > rightFile.length())
                return LEFT_TAG;
            else
                return RIGHT_TAG;
        }
        return null;
    }

    private TaggedMapOutput generateTaggedMapOutput(Object key, Object value) {
        TaggedMapOutput aRecord = generateTaggedMapOutput(value);
        if (aRecord != null)
            ((TaggedEntry) aRecord).setKey((Text) key);
        return aRecord;
    }

    public void map(Object key, Object value,
                    OutputCollector output, Reporter reporter) throws IOException {
        if (this.reporter == null) {
            this.reporter = reporter;
        }
        addLongValue("totalCount", 1);
        TaggedMapOutput aRecord = generateTaggedMapOutput(key, value);
        if (aRecord == null) {
            addLongValue("discardedCount", 1);
            return;
        }
        Text groupKey = generateGroupKey(aRecord);
        if (groupKey == null) {
            addLongValue("nullGroupKeyCount", 1);
            return;
        }
        output.collect(groupKey, aRecord);
        addLongValue("collectedCount", 1);
    }

    @Override
    protected TaggedMapOutput generateTaggedMapOutput(Object value) {
        TaggedEntry retv = new TaggedMatrix();
        retv.setTag(this.inputTag);
        retv.setData((Writable) value);
        return retv;
    }

}
