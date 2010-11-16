package reconcile.hbase.mapreduce.annotation;

import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.metaCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import reconcile.Constructor;
import reconcile.SystemConfig;
import reconcile.classifiers.Classifier;
import reconcile.clusterers.Clusterer;
import reconcile.clusterers.ThresholdClusterer;
import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.data.Document;
import reconcile.featureVector.Feature;
import reconcile.featureVector.FeatureWriter;
import reconcile.featureVector.FeatureWriterARFF;
import reconcile.featureVector.FeatureWriterARFFBinarized;
import reconcile.featureVector.individualFeature.DocNo;
import reconcile.features.properties.Property;
import reconcile.filter.PairGenerator;
import reconcile.filter.SmartInstanceGenerator;
import reconcile.general.Constants;
import reconcile.general.Utils;
import reconcile.hbase.ReconcileDocument;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.table.DocSchema;

public class CorefResolver
    extends ChainableAnnotationJob {

public static final String COREF_FEATURES = "coref_features";

public static final String CONFIG_FILE = "Coreference_Config_File";

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>optional start row
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new CorefResolver(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

@Override
public void init(String[] args, Job job, Scan scan)
{
  String startRow = null;
  if (args.length > 0) {
    startRow = args[0];
  }
  setSource(args, job);

  job.getConfiguration().set("mapred.tasktracker.map.tasks.maximum", "5");

  scan.addColumn(textCF.getBytes(), textRaw.getBytes());
  scan.addFamily(annotationsCF.getBytes());
  scan.addFamily(metaCF.getBytes());

  if (startRow != null) {
    scan.setStartRow(startRow.getBytes());
  }
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return FGMapper.class;
}

public static class FGMapper
    extends AnnotateMapper {



private static final String MODEL_NAME = "MODEL_NAME";

PairGenerator pairGen;

Context context;

SystemConfig cfg;

List<Feature> featureList;

private Classifier learner;

private String[] options;

private Clusterer clusterer;

private String[] clustOptions;

int docNo = 0;

public FGMapper() {

  try {
    pairGen = new SmartInstanceGenerator();
  }
  catch (Exception e) {
    e.printStackTrace();
    LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

}

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
    this.context = context;
    String configFile = context.getConfiguration().get(CONFIG_FILE, "/config.default");
    URL res = Utils.class.getResource(configFile);
    SystemConfig defaultConfig = new SystemConfig(res.toURI().toString());

    Utils.setConfig(defaultConfig);
    cfg = Utils.getConfig();
    String[] featureNames = cfg.getFeatureNames();
    List<String> fList = Lists.newArrayList(featureNames);
    // for (Iterator<String> i = fList.iterator(); i.hasNext();) {
    // String f = i.next();
    // if (f.equals("instClass")) {
    // i.remove();
    // }
    // }

    featureList = Constructor.createFeatures(fList.toArray(new String[fList.size()]));

    String modelFN = cfg.getString(MODEL_NAME);
    String classifier = cfg.getClassifier();
    if (classifier == null) throw new RuntimeException("Classifier not specified");
    options = cfg.getStringArray("TesterOptions." + classifier);

    System.out.println("model work dir:" + cfg.getString("WORK_DIR"));
    String fullModelFN = Utils.getWorkDirectory() + "/" + modelFN;
    System.out.println("full model file:" + fullModelFN);
    String modelName = context.getConfiguration().get(MODEL_NAME, "/reconcile.classifiers.PerceptronM.model");
    System.out.println("modelName:" + modelName);

    learner = Constructor.createClassifier(classifier, modelName);
    System.out.println("classifier:" + learner.getName() + ", class:" + learner.getClass().getName());

    String clustererName = cfg.getClusterer();
    clusterer = Constructor.createClusterer(clustererName);
    if (clusterer instanceof ThresholdClusterer) {
      String thr = cfg.getString("ClustOptions.THRESHOLD");
      if (thr != null && thr.length() > 0) {
        ((ThresholdClusterer) clusterer).setThreshold(Double.parseDouble(thr));
      }
    }
    clustOptions = cfg.getStringArray("ClustOptions." + clustererName);

  }
  catch (IOException e) {
    e.printStackTrace();
  }
  catch (InterruptedException e) {
    e.printStackTrace();
  }
  catch (ConfigurationException e) {
    e.printStackTrace();
  }
  catch (URISyntaxException e) {
    e.printStackTrace();
  }

}

@Override
public void map(ImmutableBytesWritable mapKey, Result row, Context context)
    throws IOException, InterruptedException
{
  context.getCounter("Coref Feature Generator", "row counter").increment(1);

  // keep alive for long parses
  ReportProgressThread progress = null;
  try {
    AnnotationSet coref = DocSchema.getAnnotationSet(row, Constants.RESPONSE_NPS);
    if (coref != null && coref.size() > 0) {
      context.getCounter("Coref Resolver", "skip -- already processed").increment(1);
      return;
    }

    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 1000);
    makeFeatures(row);
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }
}

private AnnotationSet makeFeatures(Result row)
{
  Document doc = new ReconcileDocument(row);
  StringWriter stringOutput = new StringWriter();
  PrintWriter output = new PrintWriter(stringOutput, true);
  boolean write_binary = Utils.getConfig().getBoolean("WRITE_BINARIZED_FEATURE_FILE", true);
  FeatureWriter writer;
  if (write_binary) {
    writer = new FeatureWriterARFFBinarized(featureList, output);
  }
  else {
    writer = new FeatureWriterARFF(featureList, output);
  }

  String docId = Bytes.toString(row.getRow());
  docNo = (docNo++) % Short.MAX_VALUE;
  addDocNo(doc, String.valueOf(docNo));

  writer.printHeader();
  AnnotationSet basenp = doc.getAnnotationSet(Constants.NP);
  //AnnotationSet basenp = new AnnotationReaderBytespan().read(doc.getAnnotationDir(), Constants.PROPERTIES_FILE_NAME);
  Annotation[] basenpArray = basenp.toArray();
  System.out.println("Document " + docId + ": " + " (" + basenpArray.length + " nps)");


  // Initialize the pair generator with the new document (training == false)
  pairGen.initialize(basenpArray, doc, false);

  while (pairGen.hasNext()) {
    Annotation[] pair = pairGen.nextPair();
    Annotation np1 = pair[0], np2 = pair[1];
    HashMap<Feature, String> values = makeVectorTimed(np1, np2, featureList, doc);
    writer.printInstanceVector(values);
  }
  // outputNPProperties(doc, basenp, row);

  String predictionString = stringOutput.toString();
  System.out.println(predictionString);
  classify(doc, predictionString, row);

  return basenp;
}

/**
 *
 * @param doc
 * @param arff
 *          the feature vector
 * @param row
 */
private void classify(Document doc, String arff, Result row)
{
  StringWriter predictionStringOut = new StringWriter();
  learner.test(new StringReader(arff), predictionStringOut, options);

  Reader predictionIn = new StringReader(predictionStringOut.toString());
  AnnotationSet ces = doc.getAnnotationSet(Constants.NP);
  AnnotationSet clusterResult = clusterer.cluster(ces, predictionIn, clustOptions);

  try {
    Put put = new Put(doc.getDocumentId().getBytes());
    addAnnotation(row, put, clusterResult, clusterResult.getName());
    docTable.put(put);
    context.getCounter("FPMapper", "Result put").increment(1);
  }
  catch (IOException e) {
    context.getCounter("FGMapper", "Result put IO error").increment(1);
    e.printStackTrace();
  }

}

private void addDocNo(Document doc, String docId)
{

  AnnotationSet docNo = doc.getAnnotationSet(DocNo.ID);
  if (docNo == null) {
    docNo = new AnnotationSet(DocNo.ID);
  }
  if (docNo.size() == 0) {
    Map<String, String> m = Maps.newHashMap();
    m.put(DocNo.ID, docId);
    Annotation a = new Annotation(0, 0, doc.length(), DocNo.ID, m);
    docNo.add(a);
    doc.writeAnnotationSet(docNo);
  }
}

public HashMap<Feature, String> makeVectorTimed(Annotation np1, Annotation np2, List<Feature> featureList,
    Document doc)
{
  HashMap<Feature, String> result = new HashMap<Feature, String>();
  for (Feature feat : featureList) {
    long stTime = System.currentTimeMillis();
    feat.getValue(np1, np2, doc, result);
    long elapsedTime = System.currentTimeMillis() - stTime;
    context.getCounter("Feature Timer", feat.getName()).increment(elapsedTime);

  }
  return result;
}

private void outputNPProperties(Document doc, AnnotationSet nps, Result row)
{
  // Output all the NP properties that were computed
  AnnotationSet properties = new AnnotationSet(Constants.PROPERTIES_FILE_NAME);

  for (Annotation np : nps) {
    Map<String, String> npProps = new TreeMap<String, String>();
    Map<Property, Object> props = np.getProperties();
    if (props != null && props.keySet() != null) {
      for (Property p : props.keySet()) {
        npProps.put(p.toString(), Utils.printProperty(props.get(p)));
      }
    }

    String num = np.getAttribute(Constants.CE_ID);
    npProps.put(Constants.CE_ID, num);
    npProps.put("Text", doc.getAnnotText(np));
    properties.add(np.getStartOffset(), np.getEndOffset(), "np", npProps);
  }

  doc.writeAnnotationSet(properties);
  try {
  Put put = new Put(doc.getDocumentId().getBytes());
  addAnnotation(row, put, properties, properties.getName());
    docTable.put(put);
    context.getCounter("FPMapper", "put").increment(1);
  }
  catch (IOException e) {
    context.getCounter("FGMapper", "put IO error").increment(1);
    e.printStackTrace();
  }

}

}
}
