package nca;

import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public abstract class EntryJoinReducerBase extends DataJoinReducerBase {

    @Override
    protected void collect(Object key, TaggedMapOutput aRecord, OutputCollector output, Reporter reporter)
            throws IOException {
        super.collect(((TaggedEntry)aRecord).getKey(), aRecord, output, reporter);
    }

}
