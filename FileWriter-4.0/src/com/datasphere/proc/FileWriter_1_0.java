package com.datasphere.proc;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.constants.Constant;
import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.io.dynamicdirectory.DynamicDirectoryOutputStreamGenerator;
import com.datasphere.io.file.persistence.util.FileMetadataPersistenceManager;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.recovery.Acknowledgeable;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.components.Target;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.source.lib.intf.OutputStreamProvider;
import com.datasphere.source.lib.intf.RollOverObserver;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.rollingpolicy.outputstream.RollOverOutputStream;
import com.datasphere.source.lib.rollingpolicy.property.RollOverProperty;
import com.datasphere.source.lib.rollingpolicy.util.RollOverOutputStreamFactory;
import com.datasphere.source.lib.rollingpolicy.util.RollingPolicyUtil;
import com.datasphere.source.lib.rollingpolicy.util.RolloverFilenameFormat;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;

@PropertyTemplate(name = "FileWriter", type = AdapterType.target, properties = {
		@PropertyTemplateProperty(name = "filename", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "directory", type = String.class, required = false, defaultValue = ""),
		@PropertyTemplateProperty(name = "rolloveronddl", type = Boolean.class, required = false, defaultValue = "true"),
		@PropertyTemplateProperty(name = "rolloverpolicy", type = String.class, required = false, defaultValue = "eventcount:10000,interval:30s"),
		@PropertyTemplateProperty(name = "flushpolicy", type = String.class, required = false, defaultValue = "eventcount:10000,interval:30"),
		@PropertyTemplateProperty(name = "compressiontype", type = String.class, required = false, defaultValue = "") }, outputType = HDEvent.class, requiresFormatter = true)
public class FileWriter_1_0 extends BaseWriter implements RollOverObserver, Acknowledgeable {
	private static final String ROLL_OVER_POLICY = "rolloverpolicy";
	protected boolean append;
	protected boolean createLocalDirectory;
	private String fileName;
	private String directory;
	private FileOutputStream fileOutputStream;
	private OutputStream outputStream;
	private Logger logger;
	private int flushInterval;
	private int flushCount;
	private int flushCounter;
	private Timer flushTimer;
	private boolean flushIntervalEnabled;
	private boolean flushCountEnabled;
	private RolloverFilenameFormat filenameFormat;
	protected RollOverProperty rollOverProperty;
	private boolean writerClosed;
	private Position currentPosition;
	private boolean acknowledgePosition;
	private boolean compressionEnabled;
	private int ackEventCounter;
	private boolean rollOverOnDDL;
	private String lastRolledOverFilename;
	private String lastRolledOverTime;
	private String lastCheckpointedPosition;
	private String currentFilename;
	private long totalEventsInCurrentFile;
	private long totalEventsInPreviousFile;
	private long eventCounter;
	private DynamicDirectoryOutputStreamGenerator dynamicDirectoryOutputStreamGenerator;
	private boolean rolledoverOnDDL;
	private FileMetadataPersistenceManager fileMetadataPersistenceManager;
	private String componentName;
	protected long externalFileCreationTime;
	private long eventTimestamp;
	private boolean firstEvent;
	private UUID componentUUID;
	private String distributionID;
	protected boolean fileMetadataRepositoryEnabled;
	private boolean bufferingEnabled;

	public FileWriter_1_0() {
		this.append = false;
		this.createLocalDirectory = true;
		this.logger = Logger.getLogger((Class) FileWriter_1_0.class);
		this.flushCounter = 0;
		this.flushIntervalEnabled = false;
		this.flushCountEnabled = false;
		this.writerClosed = false;
		this.acknowledgePosition = false;
		this.compressionEnabled = false;
		this.ackEventCounter = 0;
		this.rollOverOnDDL = true;
		this.lastRolledOverFilename = " N/A";
		this.lastRolledOverTime = " N/A";
		this.lastCheckpointedPosition = " N/A";
		this.currentFilename = " N/A";
		this.totalEventsInCurrentFile = 0L;
		this.totalEventsInPreviousFile = 0L;
		this.eventCounter = 0L;
		this.rolledoverOnDDL = false;
		this.fileMetadataPersistenceManager = null;
		this.componentName = null;
		this.externalFileCreationTime = 0L;
		this.eventTimestamp = 0L;
		this.firstEvent = false;
		this.componentUUID = null;
		this.distributionID = null;
		this.fileMetadataRepositoryEnabled = false;
		this.bufferingEnabled = false;
	}

	public void init(final Map<String, Object> writerProperties, final Map<String, Object> formatterProperties,
			final UUID inputStream, String distributionID) throws Exception {
		super.init((Map) writerProperties, (Map) formatterProperties, inputStream, distributionID);
		final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
		localPropertyMap.putAll(writerProperties);
		localPropertyMap.putAll(formatterProperties);
		final Property prop = new Property((Map) localPropertyMap);
		String rollingPolicyName = prop.getString("rolloverpolicy", (String) null);
		if (rollingPolicyName != null) {
			localPropertyMap.putAll(RollingPolicyUtil.mapPolicyValueToRollOverPolicyName(rollingPolicyName));
		}
		this.directory = prop.getString("directory", "");
		final Target target = (Target) this.receiptCallback;
		this.componentName = target.getMetaInfo().getFullName();
		this.componentUUID = target.getMetaInfo().getUuid();
		distributionID = target.getFlow().getDistributionId();
		if (distributionID != null) {
			this.distributionID = distributionID;
		} else {
			this.distributionID = HazelcastSingleton.getBindingInterface();
		}
		this.fileMetadataPersistenceManager = new FileMetadataPersistenceManager(this.componentName, this.componentUUID,
				this.distributionID, (Map) localPropertyMap);
		(this.dynamicDirectoryOutputStreamGenerator = this.createDynamicDirectoryOutputStreamGenerator())
				.setFileMetadataPersistenceManager(this.fileMetadataPersistenceManager);
		this.fileName = prop.getString("filename", (String) null);
		if (this.fileName == null) {
			throw new AdapterException("File name cannot be null, please specify a valid file name");
		}
		if (this.fileName.trim().isEmpty()) {
			throw new AdapterException("File name cannot be empty, please specify a valid file name");
		}
		if (this.directory != null && !this.directory.trim().isEmpty()) {
			final String separator = this.getSeparator();
			if (!this.directory.endsWith(separator)) {
				this.directory += separator;
			}
			this.dynamicDirectoryOutputStreamGenerator.identifyTokens(this.fields, this.directory);
			if (!this.dynamicDirectoryOutputStreamGenerator.enabled() && this.createLocalDirectory) {
				final File directoryToBeCreated = new File(this.directory);
				if (!directoryToBeCreated.exists() && !directoryToBeCreated.mkdirs()) {
					throw new AdapterException("Failure in writer initialization. Directory " + this.directory
							+ " doesn't exist and unable to create it");
				}
			}
		}
		if (!this.dynamicDirectoryOutputStreamGenerator.enabled()) {
			this.rollOverProperty = this.fileMetadataPersistenceManager.getRolloverProperty(this.directory);
			this.append = this.rollOverProperty.shouldAppend();
			this.filenameFormat = new RolloverFilenameFormat(this.fileName, this.rollOverProperty.getFileLimit(),
					this.rollOverProperty.getSequenceStart(), this.rollOverProperty.getIncrementSequenceBy(),
					this.rollOverProperty.shouldAddDefaultSequence());
		}
		final String flushPolicy = prop.getString("flushpolicy", (String) null);
		if (flushPolicy != null) {
			final Map<String, Object> flushProperties = this.extractFlushProperties(flushPolicy);
			final Property flushProperty = new Property((Map) flushProperties);
			this.flushInterval = flushProperty.getInt("flushinterval", 0);
			if (this.flushInterval < 0) {
				this.flushInterval = -this.flushInterval;
			}
			this.flushInterval *= 1000;
			if (this.flushInterval != 0) {
				this.flushIntervalEnabled = true;
			}
			this.flushCount = flushProperty.getInt("flushcount", 0);
			if (this.flushCount < 0) {
				this.flushCount = -this.flushCount;
			}
			if (this.flushCount != 0) {
				this.flushCountEnabled = true;
			}
		}
		if (this.flushIntervalEnabled) {
			this.initializeFlushTimer();
		}
		final String compressionType = prop.getString("compressiontype", (String) null);
		if (compressionType != null && !compressionType.trim().isEmpty()) {
			this.compressionEnabled = true;
			if (!compressionType.equalsIgnoreCase("GZIP")) {
				this.logger.warn((Object) ("Unsupported compression type " + compressionType
						+ " is specified. Defaulting to supported compression type : GZIP."));
			}
		}
		if (this.logger.isTraceEnabled()) {
			this.logger.trace((Object) ("FileWriter is initialized with following properties\nFileName - ["
					+ this.fileName + "]\nDirectory - [" + this.directory + "]\nRollingPolicy - [" + rollingPolicyName
					+ "]\nFlushPolicy - [" + flushPolicy + "]"));
		}
		if (this.receiptCallback != null) {
			this.acknowledgePosition = true;
		}
		this.writerClosed = false;
		this.rollOverOnDDL = prop.getBoolean("rolloveronddl", true);
		if (this.rollOverOnDDL) {
			final String eventType = prop.getString("EventType", (String) null);
			if (eventType != null && !eventType.isEmpty()) {
				if (!eventType.equalsIgnoreCase("HDEvent")) {
					this.rollOverOnDDL = false;
					this.logger.warn((Object) "Rollover on DDL is only supported for incoming stream of type HDEvent.");
				}
			} else {
				this.rollOverOnDDL = false;
				this.logger.warn((Object) "Rollover on DDL is only supported for incoming stream of type HDEvent.");
			}
		}
		this.fileMetadataRepositoryEnabled = Boolean
				.valueOf(System.getProperty("com.datasphere.config.trackFileLineageMetadata", "false"));
		this.bufferSize = prop.getInt("buffersize", 0);
		if (this.bufferSize > 0) {
			this.bufferingEnabled = true;
		}
		this.open();
	}

	private void open() throws AdapterException {
		try {
			final RollOverOutputStream.OutputStreamBuilder outputStreamBuilder = (RollOverOutputStream.OutputStreamBuilder) new RollOverOutputStream.OutputStreamBuilder() {
				public OutputStream buildOutputStream(final RolloverFilenameFormat filenameFormat) throws IOException {
					final OutputStream outputStream = FileWriter_1_0.this.compressionEnabled
							? new GZIPOutputStream(FileWriter_1_0.this
									.getOutputStream(FileWriter_1_0.this.directory + filenameFormat.getNextSequence()))
							: FileWriter_1_0.this
									.getOutputStream(FileWriter_1_0.this.directory + filenameFormat.getNextSequence());
					if (FileWriter_1_0.this.formatter instanceof OutputStreamProvider) {
						return ((OutputStreamProvider) FileWriter_1_0.this.formatter).provideOutputStream(outputStream);
					}
					return outputStream;
				}
			};
			if (this.dynamicDirectoryOutputStreamGenerator.enabled()) {
				this.directory = "";
				this.dynamicDirectoryOutputStreamGenerator.setOutputStreamBuilder(outputStreamBuilder);
				this.dynamicDirectoryOutputStreamGenerator.setFormatter(this.formatter);
			} else {
				this.outputStream = (OutputStream) RollOverOutputStreamFactory
						.getRollOverOutputStream(this.rollOverProperty, outputStreamBuilder, this.filenameFormat);
				this.notifyOnInitialOpen();
				if (this.outputStream instanceof RollOverOutputStream) {
					((RollOverOutputStream) this.outputStream).registerObserver((RollOverObserver) this);
				}
				final byte[] bytesToBeWrittenOnOpen = this.formatter.addHeader();
				if (bytesToBeWrittenOnOpen != null) {
					this.write(bytesToBeWrittenOnOpen);
				}
			}
			if (this.logger.isTraceEnabled() && !this.dynamicDirectoryOutputStreamGenerator.enabled()) {
				this.logger.trace(
						(Object) ("File " + this.filenameFormat.getCurrentFileName() + " is opened in the directory "
								+ new File(this.filenameFormat.getCurrentFileName()).getCanonicalPath()));
			}
		} catch (Exception e) {
			final AdapterException se = new AdapterException(Error.GENERIC_EXCEPTION, (Throwable) e);
			throw se;
		}
	}

	public void receive(final int channel, final Event event, final Position pos) throws Exception {
		if (this.dynamicDirectoryOutputStreamGenerator.enabled()) {
			final OutputStream outputStream = this.dynamicDirectoryOutputStreamGenerator.getDynamicOutputStream(event);
			if (outputStream == null) {
				return;
			}
			this.outputStream = outputStream;
		}
		synchronized (this.outputStream) {
			this.currentPosition = pos;
			this.receiveImpl(channel, event);
		}
	}

	public void receiveImpl(final int channel, final Event event) throws Exception {
		String directoryName = this.directory;
		long eventCount;
		if (this.dynamicDirectoryOutputStreamGenerator.enabled()) {
			final OutputStream outputStream = this.dynamicDirectoryOutputStreamGenerator.getDynamicOutputStream(event);
			if (outputStream == null) {
				return;
			}
			directoryName = this.dynamicDirectoryOutputStreamGenerator.getCurrentWorkingDirectoryName();
			this.filenameFormat = this.dynamicDirectoryOutputStreamGenerator.getCachedFilenameFormat(directoryName);
			eventCount = this.dynamicDirectoryOutputStreamGenerator
					.getEvenCounter(directoryName + this.filenameFormat.getCurrentFileName());
			this.outputStream = outputStream;
		} else {
			++this.eventCounter;
			eventCount = this.eventCounter;
		}
		synchronized (this.outputStream) {
			try {
				if (this.rollOverOnDDL) {
					final HDEvent hdevent = (HDEvent) event;
					final String operationType = hdevent.metadata.get("OperationType") == null ? null
							: hdevent.metadata.get("OperationType").toString();
					if (operationType != null && !operationType.isEmpty()
							&& operationType.equals(Constant.DDL_OPERATION)) {
						if (this.flushCountEnabled || this.flushIntervalEnabled) {
							this.sync();
						}
						this.rolledoverOnDDL = true;
						((RollOverOutputStream) this.outputStream).rollover();
						this.rolledoverOnDDL = false;
						if (this.acknowledgePosition) {
							this.receiptCallback.ack(this.flushCount, this.currentPosition);
						}
						return;
					}
				}
				this.eventTimestamp = event.getTimeStamp();
				if (!this.firstEvent) {
					final FileMetadataExtension fileMetadataExtension = this.fileMetadataPersistenceManager
							.getFileMetadataExtension(directoryName + this.filenameFormat.getCurrentFileName());
					fileMetadataExtension.setFirstEventTimestamp(this.eventTimestamp);
					fileMetadataExtension.setStatus(FileMetadataExtension.Status.PROCESSING.toString());
					this.fileMetadataPersistenceManager.updateFileMetadataEntry(fileMetadataExtension);
					this.firstEvent = true;
//					System.out.println("------file:"+this.directory + this.filenameFormat.getPreFileName());
//					if(this.filenameFormat.getPreFileName() != null) {
//						File file = new File(this.directory +this.filenameFormat.getPreFileName());
//						File destFile = new File(this.directory +this.filenameFormat.getPreFileName() + ".completed");
//						file.renameTo(destFile);
//					}
				}
				this.totalEventsInCurrentFile = eventCount;
				final byte[] bytesToBeWritten = this.formatter.format((Object) event);
				this.write(bytesToBeWritten);
			} catch (Exception e) {
				throw new AdapterException("Failure in writing event to the file. ", (Throwable) e);
			}
		}
	}

	public void close() throws AdapterException {
		if (this.outputStream != null) {
			synchronized (this.outputStream) {
				this.writerClosed = true;
				this.firstEvent = false;
				Label_0075: {
					if (!this.flushCountEnabled) {
						if (!this.flushIntervalEnabled) {
							break Label_0075;
						}
					}
					try {
						this.sync();
					} catch (IOException e) {
						this.logger.error((Object) ("Failure in synchronizing data to file. " + e.getMessage()));
					}
					try {
						if (this.dynamicDirectoryOutputStreamGenerator.enabled()) {
							for (final Map.Entry<String, OutputStream> entry : this.dynamicDirectoryOutputStreamGenerator
									.getOutputStreamCache().entrySet()) {
								final OutputStream outputStream = entry.getValue();
								if (outputStream != null) {
									(this.outputStream = outputStream).close();
									final String directoryName = entry.getKey();
									this.filenameFormat = this.dynamicDirectoryOutputStreamGenerator
											.getCachedFilenameFormat(directoryName);
									this.updateFileMetadataEntry(
											directoryName + this.filenameFormat.getCurrentFileName(),
											FileMetadataExtension.ReasonForRollOver.NONE);
									this.acknowledgePosition(directoryName, this.filenameFormat.getCurrentFileName());
								}
							}
							this.dynamicDirectoryOutputStreamGenerator.clear();
						} else {
							this.outputStream.close();
							this.updateFileMetadataEntry(this.directory + this.filenameFormat.getCurrentFileName(),
									FileMetadataExtension.ReasonForRollOver.NONE);
							this.acknowledgePosition(this.directory, this.filenameFormat.getCurrentFileName());
						}
						if (this.logger.isTraceEnabled()) {
							this.logger.trace(
									(Object) ("File " + this.filenameFormat.getCurrentFileName() + " is closed"));
						}
//						if(this.filenameFormat.getPreFileName() != null) {
//							File file = new File(this.directory +this.filenameFormat.getPreFileName());
//							File destFile = new File(this.directory +this.filenameFormat.getPreFileName() + ".completed");
//							file.renameTo(destFile);
//						}
					} catch (Exception e2) {
						this.logger.error((Object) ("Falilure in closing/stopping FileWriter " + e2.getMessage()));
					} finally {
						if (this.flushTimer != null) {
							this.stopFlushTimer();
						}
						this.eventCounter = 0L;
						if (this.fileMetadataPersistenceManager != null) {
							this.fileMetadataPersistenceManager.clear();
						}
					}
				}
			}
		}
	}

	protected OutputStream getOutputStream(final String filename) throws IOException {
		final File file = new File(filename);
		if (this.fileMetadataRepositoryEnabled && file.exists()) {
			String directoryName = file.getParent();
			if (directoryName == null) {
				directoryName = new File(".").getCanonicalPath();
			}
			throw new IOException("File " + file.getName() + " already exists in the current directory " + directoryName
					+ ". Please archive the existing files in this directory starting from this file sequence.");
		}
		this.fileOutputStream = new FileOutputStream(file, this.append);
		this.externalFileCreationTime = Files
				.readAttributes(file.toPath(), BasicFileAttributes.class, new LinkOption[0]).creationTime().toMillis();
		if (this.bufferingEnabled) {
			return new BufferedOutputStream(this.fileOutputStream, this.bufferSize);
		}
		return this.fileOutputStream;
	}

	private void write(final byte[] bytesToBeWritten) throws Exception {
		try {
			this.outputStream.write(bytesToBeWritten);
			++this.ackEventCounter;
			if (this.flushCountEnabled) {
				this.flush();
			}
		} catch (IOException e) {
			if (!this.writerClosed) {
				throw e;
			}
			this.logger.error((Object) "Failure in writing data to file. FileWriter is closed.");
		}
		if (this.logger.isTraceEnabled()) {
			this.logger.trace((Object) ("No of bytes written in the file " + this.filenameFormat.getCurrentFileName()
					+ " is " + bytesToBeWritten.length));
		}
	}

	public void flush() throws IOException {
		++this.flushCounter;
		if (this.flushCount == this.flushCounter) {
			this.sync();
			if (this.acknowledgePosition && !this.fileMetadataRepositoryEnabled) {
				this.receiptCallback.ack(this.ackEventCounter, this.currentPosition);
				this.lastCheckpointedPosition = " " + this.currentPosition;
			}
			this.resetFlushCounter();
			this.ackEventCounter = 0;
			if (this.flushIntervalEnabled) {
				this.stopFlushTimer();
				this.initializeFlushTimer();
			}
		}
	}

	private void resetFlushCounter() {
		this.flushCounter = 0;
	}

	protected void sync() throws IOException {
		this.outputStream.flush();
		this.fileOutputStream.getFD().sync();
	}

	private void initializeFlushTimer() {
		(this.flushTimer = new Timer()).schedule(new FlushTask(), this.flushInterval, this.flushInterval);
	}

	private void stopFlushTimer() {
		this.flushTimer.cancel();
		this.flushTimer = null;
	}

	private boolean isFlushingCountEnabled() {
		return this.flushCountEnabled;
	}

	private Map<String, Object> extractFlushProperties(final String flushPolicy) throws AdapterException {
		final Map<String, Object> flushProperties = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
		final String[] split;
		final String[] flushPolicies = split = flushPolicy.split(",");
		for (final String specifiedFlushPolicy : split) {
			final String[] policyProperties = specifiedFlushPolicy.split(":");
			if (policyProperties.length > 1) {
				if (!policyProperties[0].contains("flush")) {
					if (policyProperties[0].contains("event")) {
						policyProperties[0] = policyProperties[0].replace("event", "flush");
					} else {
						policyProperties[0] = "flush" + policyProperties[0];
					}
				}
				flushProperties.put(policyProperties[0], policyProperties[1]);
			} else if (!policyProperties[0].trim().isEmpty()) {
				if (!policyProperties[0].contains("flush")) {
					if (policyProperties[0].contains("event")) {
						policyProperties[0] = policyProperties[0].replace("event", "flush");
					} else {
						policyProperties[0] = "flush" + policyProperties[0];
					}
				}
				flushProperties.put(policyProperties[0], null);
			}
		}
		return flushProperties;
	}

	public void preRollover() throws IOException {
		try {
			if (this.flushCountEnabled || this.flushIntervalEnabled) {
				this.sync();
			}
			final byte[] bytesToBeAppended = this.formatter.addFooter();
			if (bytesToBeAppended != null) {
				this.write(bytesToBeAppended);
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public void postRollover(final String lastRolledOverFilename) throws IOException {
		try {
			if (!this.writerClosed) {
				this.firstEvent = false;
				String directoryName = this.directory;
				long eventCount;
				if (this.dynamicDirectoryOutputStreamGenerator.enabled()) {
					directoryName = this.dynamicDirectoryOutputStreamGenerator.getCurrentWorkingDirectoryName();
					this.filenameFormat = this.dynamicDirectoryOutputStreamGenerator
							.getCachedFilenameFormat(directoryName);
					eventCount = this.dynamicDirectoryOutputStreamGenerator
							.getEvenCounter(directoryName + lastRolledOverFilename);
				} else {
					eventCount = this.eventCounter;
				}
				this.lastRolledOverFilename = " " + lastRolledOverFilename;
				this.lastRolledOverTime = " " + new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss").format(new Date());
				this.currentFilename = " " + this.filenameFormat.getCurrentFileName();
				this.totalEventsInPreviousFile = eventCount;
				FileMetadataExtension.ReasonForRollOver reason = FileMetadataExtension.ReasonForRollOver.ROLLOVER_EXPIRY;
				if (this.rolledoverOnDDL) {
					reason = FileMetadataExtension.ReasonForRollOver.DDL;
				}
				this.updateFileMetadataEntry(directoryName + lastRolledOverFilename, reason);
				this.acknowledgePosition(directoryName, lastRolledOverFilename);
				this.createFileMetadataEntry(directoryName + this.filenameFormat.getCurrentFileName());
				this.eventCounter = 0L;
				this.totalEventsInCurrentFile = this.eventCounter;
				final byte[] bytesToBeWrittenOnOpen = this.formatter.addHeader();
				if (bytesToBeWrittenOnOpen != null) {
					this.write(bytesToBeWrittenOnOpen);
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public void publishMonitorEvents(final MonitorEventsCollection events) {
		events.add(MonitorEvent.Type.LAST_ROLLEDOVER_FILENAME, this.lastRolledOverFilename);
		events.add(MonitorEvent.Type.LAST_ROLLEDOVER_TIME, this.lastRolledOverTime);
		events.add(MonitorEvent.Type.LAST_CHECKPOINTED_POSITION, this.lastCheckpointedPosition);
		events.add(MonitorEvent.Type.CURRENT_FILENAME, this.currentFilename);
		events.add(MonitorEvent.Type.TOTAL_EVENTS_IN_CURRENT_FILE, this.totalEventsInCurrentFile);
		events.add(MonitorEvent.Type.TOTAL_EVENTS_IN_PREVIOUS_FILE, this.totalEventsInPreviousFile);
	}

	protected DynamicDirectoryOutputStreamGenerator createDynamicDirectoryOutputStreamGenerator() {
		return new DynamicDirectoryOutputStreamGenerator((RollOverObserver) this);
	}

	protected String getSeparator() {
		return FileSystems.getDefault().getSeparator();
	}

	private void createFileMetadataEntry(final String key) {
		final FileMetadataExtension fileMetadataExtension = this.fileMetadataPersistenceManager
				.getFileMetadataExtension(key);
		fileMetadataExtension.setCreationTimeStamp(System.currentTimeMillis());
		fileMetadataExtension.setDirectoryName(this.dynamicDirectoryOutputStreamGenerator.enabled()
				? this.dynamicDirectoryOutputStreamGenerator.getCurrentWorkingDirectoryName()
				: this.directory);
		fileMetadataExtension.setFileName(this.filenameFormat.getCurrentFileName());
		fileMetadataExtension.setParentComponent(this.componentName);
		fileMetadataExtension.setStatus(FileMetadataExtension.Status.CREATED.toString());
		fileMetadataExtension.setSequenceNumber(this.filenameFormat.getCurrentSequenceNo());
		fileMetadataExtension.setExternalFileCreationTime(this.externalFileCreationTime);
		fileMetadataExtension.setParentComponentUUID(this.componentUUID);
		fileMetadataExtension.setDistributionID(this.distributionID);
		this.fileMetadataPersistenceManager.createFileMetadataEntry(fileMetadataExtension);
	}

	private void updateFileMetadataEntry(final String key, final FileMetadataExtension.ReasonForRollOver reason) {
		final FileMetadataExtension fileMetadataExtension = this.fileMetadataPersistenceManager
				.getFileMetadataExtension(key);
		if (fileMetadataExtension.getDirectoryName() != null && fileMetadataExtension.getFileName() != null) {
			fileMetadataExtension.setNumberOfEvents(this.totalEventsInCurrentFile);
			fileMetadataExtension.setReasonForRollOver(reason.toString());
			fileMetadataExtension.setRollOverTimeStamp(System.currentTimeMillis());
			fileMetadataExtension.setStatus(FileMetadataExtension.Status.COMPLETED.toString());
			fileMetadataExtension.setLastEventTimestamp(this.eventTimestamp);
			this.fileMetadataPersistenceManager.updateFileMetadataEntry(fileMetadataExtension);
		}
	}

	private void acknowledgePosition(final String directoryName, final String filename) {
		if (this.acknowledgePosition && this.ackEventCounter > 0) {
			this.receiptCallback.ack(this.ackEventCounter, this.currentPosition);
			this.ackEventCounter = 0;
			this.lastCheckpointedPosition = " " + this.currentPosition;
			this.updateLastCheckpointedPosition(directoryName + filename);
		}
	}

	private void updateLastCheckpointedPosition(final String key) {
		final FileMetadataExtension fileMetadataExtension = this.fileMetadataPersistenceManager
				.getFileMetadataExtension(key);
		if (fileMetadataExtension.getDirectoryName() != null && fileMetadataExtension.getFileName() != null) {
			fileMetadataExtension
					.setLastEventCheckPointValue(KryoSingleton.write((Object) this.lastCheckpointedPosition, false));
			this.fileMetadataPersistenceManager.updateFileMetadataEntry(fileMetadataExtension);
		}
	}

	public void notifyOnInitialOpen() throws Exception {
		String directoryName = this.directory;
		if (this.dynamicDirectoryOutputStreamGenerator.enabled()) {
			directoryName = this.dynamicDirectoryOutputStreamGenerator.getCurrentWorkingDirectoryName();
			this.filenameFormat = this.dynamicDirectoryOutputStreamGenerator.getCachedFilenameFormat(directoryName);
		}
		this.createFileMetadataEntry(directoryName + this.filenameFormat.getCurrentFileName());
	}

	private class FlushTask extends TimerTask {
		private Logger logger;

		private FlushTask() {
			this.logger = Logger.getLogger((Class) FlushTask.class);
		}

		@Override
		public void run() {
			try {
				if (FileWriter_1_0.this.outputStream == null) {
					return;
				}
				synchronized (FileWriter_1_0.this.outputStream) {
					FileWriter_1_0.this.sync();
					if (FileWriter_1_0.this.acknowledgePosition && !FileWriter_1_0.this.fileMetadataRepositoryEnabled
							&& FileWriter_1_0.this.ackEventCounter > 0) {
						FileWriter_1_0.this.receiptCallback.ack(FileWriter_1_0.this.ackEventCounter,
								FileWriter_1_0.this.currentPosition);
						FileWriter_1_0.this.ackEventCounter = 0;
						FileWriter_1_0.this.lastCheckpointedPosition = " " + FileWriter_1_0.this.currentPosition;
					}
					if (FileWriter_1_0.this.isFlushingCountEnabled()) {
						FileWriter_1_0.this.resetFlushCounter();
					}
				}
			} catch (IOException e) {
				this.logger.error((Object) "Failure in flushing data to disk", (Throwable) e);
			}
		}
	}
	
	class SecurityAccess {
		public void disopen() {

		}
	}
}