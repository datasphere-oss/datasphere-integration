package com.datasphere.proc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.event.Event;
import com.datasphere.intf.EventSink;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.components.Flow;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "FileReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "directory", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "wildcard", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "blocksize", type = Integer.class, required = false, defaultValue = "64"),
		@PropertyTemplateProperty(name = "positionbyeof", type = Boolean.class, required = false, defaultValue = "true"),
		@PropertyTemplateProperty(name = "skipbom", type = Boolean.class, required = false, defaultValue = "true"),
		@PropertyTemplateProperty(name = "rolloverstyle", type = String.class, required = false, defaultValue = "Default"),
		@PropertyTemplateProperty(name = "compressiontype", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "includesubdirectories", type = Boolean.class, required = false, defaultValue = "false") }, inputType = HDEvent.class, requiresParser = true)
public class FileReader_1_0 extends BaseReader {
	public FileReader_1_0() {
		this.readerType = Reader.FILE_READER;
	}

	public static void main(final String[] args) {
		final Map<String, Object> mp = new HashMap<String, Object>();
		final Map<String, Object> mp2 = new HashMap<String, Object>();
		mp.put("directory", "/Theseusyang/Product/Samples/AppData");
		mp.put("wildcard", "customerdetails.csv");
		mp.put("blockSize", 64);
		mp.put("positionByEOF", false);
		mp.put("charset", "UTF-8");
		mp2.put("handler", "DSVParser");
		mp2.put("header", false);
		mp2.put("rowdelimiter", "\n");
		mp2.put("columndelimiter", ",");
		final AtomicInteger count = new AtomicInteger(0);
		final AtomicBoolean done = new AtomicBoolean(false);
		final FileReader_1_0 file = new FileReader_1_0();
		final long stime = System.currentTimeMillis();
		try {
			file.init((Map) mp, (Map) mp2, (UUID) null, BaseServer.getServerName(), (SourcePosition) null, false,
					(Flow) null);
			file.addEventSink((EventSink) new AbstractEventSink() {
				long lasttime = stime;
				int lastcount = 0;

				public void receive(final int channel, final Event event) throws Exception {
					final HDEvent wa = (HDEvent) event;
					if (wa.data != null) {
						System.out.println(wa.data[0]);
						count.incrementAndGet();
						if (count.get() != 0 && count.get() % 100 == 0) {
							final long etime = System.currentTimeMillis();
							final int diff = (int) (etime - this.lasttime);
							final int delta = count.get() - this.lastcount;
							final int rate = delta * 1000 / diff;
							this.lasttime = etime;
							this.lastcount = count.get();
						}
					} else {
						done.set(true);
					}
				}
			});
			while (!done.get()) {
				file.receive(0, (Event) null);
			}
		} catch (Exception ex) {
			try {
				file.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} finally {
			try {
				file.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}

	public String getMetadataKey() {
		return null;
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}
