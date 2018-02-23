package nca;

import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public abstract class EntryJoinMapperBase extends DataJoinMapperBase {

    private TaggedMapOutput generateTaggedMapOutput(Object key, Object value) {
        TaggedMapOutput aRecord = generateTaggedMapOutput(value);
        if (aRecord != null)
            ((TaggedEntry)aRecord).setKey((Text)key);
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
}
