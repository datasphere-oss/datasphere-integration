package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlRegistry
public class ObjectFactory
{
    public EntityMappings createEntityMappings() {
        return new EntityMappings();
    }
    
    public PersistenceUnitMetadata createPersistenceUnitMetadata() {
        return new PersistenceUnitMetadata();
    }
    
    public AccessMethods createAccessMethods() {
        return new AccessMethods();
    }
    
    public TenantDiscriminatorColumn createTenantDiscriminatorColumn() {
        return new TenantDiscriminatorColumn();
    }
    
    public Converter createConverter() {
        return new Converter();
    }
    
    public TypeConverter createTypeConverter() {
        return new TypeConverter();
    }
    
    public ObjectTypeConverter createObjectTypeConverter() {
        return new ObjectTypeConverter();
    }
    
    public StructConverter createStructConverter() {
        return new StructConverter();
    }
    
    public SequenceGenerator createSequenceGenerator() {
        return new SequenceGenerator();
    }
    
    public TableGenerator createTableGenerator() {
        return new TableGenerator();
    }
    
    public UuidGenerator createUuidGenerator() {
        return new UuidGenerator();
    }
    
    public Partitioning createPartitioning() {
        return new Partitioning();
    }
    
    public ReplicationPartitioning createReplicationPartitioning() {
        return new ReplicationPartitioning();
    }
    
    public RoundRobinPartitioning createRoundRobinPartitioning() {
        return new RoundRobinPartitioning();
    }
    
    public PinnedPartitioning createPinnedPartitioning() {
        return new PinnedPartitioning();
    }
    
    public RangePartitioning createRangePartitioning() {
        return new RangePartitioning();
    }
    
    public ValuePartitioning createValuePartitioning() {
        return new ValuePartitioning();
    }
    
    public HashPartitioning createHashPartitioning() {
        return new HashPartitioning();
    }
    
    public UnionPartitioning createUnionPartitioning() {
        return new UnionPartitioning();
    }
    
    public NamedQuery createNamedQuery() {
        return new NamedQuery();
    }
    
    public NamedNativeQuery createNamedNativeQuery() {
        return new NamedNativeQuery();
    }
    
    public NamedStoredProcedureQuery createNamedStoredProcedureQuery() {
        return new NamedStoredProcedureQuery();
    }
    
    public NamedStoredFunctionQuery createNamedStoredFunctionQuery() {
        return new NamedStoredFunctionQuery();
    }
    
    public NamedPlsqlStoredProcedureQuery createNamedPlsqlStoredProcedureQuery() {
        return new NamedPlsqlStoredProcedureQuery();
    }
    
    public NamedPlsqlStoredFunctionQuery createNamedPlsqlStoredFunctionQuery() {
        return new NamedPlsqlStoredFunctionQuery();
    }
    
    public PlsqlRecord createPlsqlRecord() {
        return new PlsqlRecord();
    }
    
    public PlsqlTable createPlsqlTable() {
        return new PlsqlTable();
    }
    
    public SqlResultSetMapping createSqlResultSetMapping() {
        return new SqlResultSetMapping();
    }
    
    public MappedSuperclass createMappedSuperclass() {
        return new MappedSuperclass();
    }
    
    public Entity createEntity() {
        return new Entity();
    }
    
    public Embeddable createEmbeddable() {
        return new Embeddable();
    }
    
    public PostPersist createPostPersist() {
        return new PostPersist();
    }
    
    public InstantiationCopyPolicy createInstantiationCopyPolicy() {
        return new InstantiationCopyPolicy();
    }
    
    public SecondaryTable createSecondaryTable() {
        return new SecondaryTable();
    }
    
    public MapKeyJoinColumn createMapKeyJoinColumn() {
        return new MapKeyJoinColumn();
    }
    
    public RangePartition createRangePartition() {
        return new RangePartition();
    }
    
    public EclipselinkCollectionTable createEclipselinkCollectionTable() {
        return new EclipselinkCollectionTable();
    }
    
    public OrderColumn createOrderColumn() {
        return new OrderColumn();
    }
    
    public CacheInterceptor createCacheInterceptor() {
        return new CacheInterceptor();
    }
    
    public BatchFetch createBatchFetch() {
        return new BatchFetch();
    }
    
    public TimeOfDay createTimeOfDay() {
        return new TimeOfDay();
    }
    
    public OneToMany createOneToMany() {
        return new OneToMany();
    }
    
    public Structure createStructure() {
        return new Structure();
    }
    
    public ClassExtractor createClassExtractor() {
        return new ClassExtractor();
    }
    
    public MapKeyColumn createMapKeyColumn() {
        return new MapKeyColumn();
    }
    
    public Table createTable() {
        return new Table();
    }
    
    public ConstructorResult createConstructorResult() {
        return new ConstructorResult();
    }
    
    public TenantTableDiscriminator createTenantTableDiscriminator() {
        return new TenantTableDiscriminator();
    }
    
    public NoSql createNoSql() {
        return new NoSql();
    }
    
    public JoinField createJoinField() {
        return new JoinField();
    }
    
    public Array createArray() {
        return new Array();
    }
    
    public ColumnResult createColumnResult() {
        return new ColumnResult();
    }
    
    public Version createVersion() {
        return new Version();
    }
    
    public EntityListeners createEntityListeners() {
        return new EntityListeners();
    }
    
    public Lob createLob() {
        return new Lob();
    }
    
    public ConversionValue createConversionValue() {
        return new ConversionValue();
    }
    
    public PostLoad createPostLoad() {
        return new PostLoad();
    }
    
    public ReturnInsert createReturnInsert() {
        return new ReturnInsert();
    }
    
    public EmptyType createEmptyType() {
        return new EmptyType();
    }
    
    public ElementCollection createElementCollection() {
        return new ElementCollection();
    }
    
    public EntityListener createEntityListener() {
        return new EntityListener();
    }
    
    public IdClass createIdClass() {
        return new IdClass();
    }
    
    public Cache createCache() {
        return new Cache();
    }
    
    public Multitenant createMultitenant() {
        return new Multitenant();
    }
    
    public DiscriminatorColumn createDiscriminatorColumn() {
        return new DiscriminatorColumn();
    }
    
    public PrePersist createPrePersist() {
        return new PrePersist();
    }
    
    public CloneCopyPolicy createCloneCopyPolicy() {
        return new CloneCopyPolicy();
    }
    
    public ValuePartition createValuePartition() {
        return new ValuePartition();
    }
    
    public GeneratedValue createGeneratedValue() {
        return new GeneratedValue();
    }
    
    public Field createField() {
        return new Field();
    }
    
    public CacheIndex createCacheIndex() {
        return new CacheIndex();
    }
    
    public PreUpdate createPreUpdate() {
        return new PreUpdate();
    }
    
    public ChangeTracking createChangeTracking() {
        return new ChangeTracking();
    }
    
    public FieldResult createFieldResult() {
        return new FieldResult();
    }
    
    public EmbeddedId createEmbeddedId() {
        return new EmbeddedId();
    }
    
    public ReadTransformer createReadTransformer() {
        return new ReadTransformer();
    }
    
    public AttributeOverride createAttributeOverride() {
        return new AttributeOverride();
    }
    
    public CascadeType createCascadeType() {
        return new CascadeType();
    }
    
    public ManyToMany createManyToMany() {
        return new ManyToMany();
    }
    
    public AssociationOverride createAssociationOverride() {
        return new AssociationOverride();
    }
    
    public Basic createBasic() {
        return new Basic();
    }
    
    public AdditionalCriteria createAdditionalCriteria() {
        return new AdditionalCriteria();
    }
    
    public PrimaryKey createPrimaryKey() {
        return new PrimaryKey();
    }
    
    public MapKey createMapKey() {
        return new MapKey();
    }
    
    public PlsqlParameter createPlsqlParameter() {
        return new PlsqlParameter();
    }
    
    public Index createIndex() {
        return new Index();
    }
    
    public BasicMap createBasicMap() {
        return new BasicMap();
    }
    
    public Transient createTransient() {
        return new Transient();
    }
    
    public Property createProperty() {
        return new Property();
    }
    
    public QueryHint createQueryHint() {
        return new QueryHint();
    }
    
    public PersistenceUnitDefaults createPersistenceUnitDefaults() {
        return new PersistenceUnitDefaults();
    }
    
    public Embedded createEmbedded() {
        return new Embedded();
    }
    
    public OneToOne createOneToOne() {
        return new OneToOne();
    }
    
    public Column createColumn() {
        return new Column();
    }
    
    public DiscriminatorClass createDiscriminatorClass() {
        return new DiscriminatorClass();
    }
    
    public StoredProcedureParameter createStoredProcedureParameter() {
        return new StoredProcedureParameter();
    }
    
    public Customizer createCustomizer() {
        return new Customizer();
    }
    
    public CollectionTable createCollectionTable() {
        return new CollectionTable();
    }
    
    public VariableOneToOne createVariableOneToOne() {
        return new VariableOneToOne();
    }
    
    public Id createId() {
        return new Id();
    }
    
    public JoinColumn createJoinColumn() {
        return new JoinColumn();
    }
    
    public WriteTransformer createWriteTransformer() {
        return new WriteTransformer();
    }
    
    public EntityResult createEntityResult() {
        return new EntityResult();
    }
    
    public Struct createStruct() {
        return new Struct();
    }
    
    public BasicCollection createBasicCollection() {
        return new BasicCollection();
    }
    
    public FetchGroup createFetchGroup() {
        return new FetchGroup();
    }
    
    public MapKeyClass createMapKeyClass() {
        return new MapKeyClass();
    }
    
    public UniqueConstraint createUniqueConstraint() {
        return new UniqueConstraint();
    }
    
    public PreRemove createPreRemove() {
        return new PreRemove();
    }
    
    public QueryRedirectors createQueryRedirectors() {
        return new QueryRedirectors();
    }
    
    public ManyToOne createManyToOne() {
        return new ManyToOne();
    }
    
    public PrimaryKeyJoinColumn createPrimaryKeyJoinColumn() {
        return new PrimaryKeyJoinColumn();
    }
    
    public Inheritance createInheritance() {
        return new Inheritance();
    }
    
    public Transformation createTransformation() {
        return new Transformation();
    }
    
    public FetchAttribute createFetchAttribute() {
        return new FetchAttribute();
    }
    
    public OptimisticLocking createOptimisticLocking() {
        return new OptimisticLocking();
    }
    
    public CopyPolicy createCopyPolicy() {
        return new CopyPolicy();
    }
    
    public PostRemove createPostRemove() {
        return new PostRemove();
    }
    
    public Attributes createAttributes() {
        return new Attributes();
    }
    
    public JoinTable createJoinTable() {
        return new JoinTable();
    }
    
    public PostUpdate createPostUpdate() {
        return new PostUpdate();
    }
}
