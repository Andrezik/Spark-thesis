# Ημερολόγιο Αλλαγών

Καταγραφή όλων των αλλαγών που γίνονται στον αρχικό κώδικα του Παναγιώτη Τσόλκα
(https://github.com/PanagiotisTslk/spark-submit-GUI) στο πλαίσιο της διπλωματικής εργασίας.

**Τρόπος τήρησης:** το αρχείο είναι *προσθετικό* (append-only). Καμία εγγραφή δεν
διαγράφεται και καμία δεν επεξεργάζεται εκ των υστέρων. Αν ένα αρχείο αλλάξει δεύτερη
φορά, καταγράφεται νέα εγγραφή — οι προηγούμενες παραμένουν ως έχουν, ώστε να είναι
ορατή η εξέλιξη. Οι νεότερες εγγραφές προστίθενται στο **τέλος** του αρχείου.

Κάθε εγγραφή περιέχει: ημερομηνία, αρχείο, τύπο αλλαγής, τι υπήρχε πριν, τι μπήκε, και
την αιτιολόγηση.

---

## [001] 2026-07-22 — `executor/Dockerfile` — Τροποποίηση

**Τύπος:** Τροποποίηση υπάρχοντος αρχείου
**Αρχείο:** `executor/Dockerfile`, γραμμές 15-21 (αρίθμηση πριν την αλλαγή)

### Αιτία

Το build του `kubernetes-executor` container δεν ολοκληρωνόταν. Το βήμα εγκατάστασης
των εξαρτήσεων (Java, Python) έτρεχε επί περισσότερα από 25 λεπτά χωρίς να τερματίζει,
εμποδίζοντας κάθε δοκιμή του συστήματος από άκρο σε άκρο.

### Πριν

```dockerfile
# Set working directory
WORKDIR /KubernetesAPI/executor

# Install Java and Python
RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -q -y openjdk-8-jdk python3-pip libsnappy-dev language-pack-en supervisor
```

### Μετά

```dockerfile
# Set working directory
WORKDIR /KubernetesAPI/executor

# Prevent apt from prompting for input (e.g. tzdata/locale) during build
ENV DEBIAN_FRONTEND=noninteractive

# Install Java and Python
RUN apt-get update && \
  apt-get install -q -y --no-install-recommends openjdk-8-jdk-headless python3-pip libsnappy-dev language-pack-en supervisor && \
  rm -rf /var/lib/apt/lists/*
```

### Ανάλυση των επιμέρους αλλαγών

| # | Αλλαγή | Σκοπός |
|---|--------|--------|
| 1 | Προσθήκη `ENV DEBIAN_FRONTEND=noninteractive` | Εμποδίζει το `apt` να ζητήσει διαδραστική είσοδο (π.χ. επιλογή timezone/locale). Μέσα σε `docker build` δεν υπάρχει χρήστης να απαντήσει, οπότε η διαδικασία κρεμάει επ' αόριστον. |
| 2 | Αφαίρεση `apt-get upgrade -y` | Αναβάθμιζε εκατοντάδες πακέτα του base image (μεταξύ άλλων `gcc-11`, 20 MB) που δεν χρησιμοποιούνται από τον executor. Δεν προσφέρει τίποτα και καθιστά το build μη αναπαραγώγιμο, αφού το αποτέλεσμα εξαρτάται από την ημερομηνία εκτέλεσης. |
| 3 | `openjdk-8-jdk` → `openjdk-8-jdk-headless` | Η πλήρης έκδοση του JDK σύρει μαζί της βιβλιοθήκες X11/GUI και γραμματοσειρές, άχρηστες σε περιβάλλον server. Η headless έκδοση επαρκεί για το `spark-submit`. |
| 4 | Προσθήκη `--no-install-recommends` | Αποτρέπει την αυτόματη εγκατάσταση «προτεινόμενων» πακέτων που δεν είναι απαραίτητα. |
| 5 | Προσθήκη `rm -rf /var/lib/apt/lists/*` | Διαγράφει το cache του apt στο ίδιο layer, μειώνοντας το τελικό μέγεθος του image. |

### Κατάσταση

Εκκρεμεί επιβεβαίωση: το build δεν έχει ακόμη ολοκληρωθεί επιτυχώς μετά την αλλαγή.

---

## [002] 2026-07-29 — `backend/SparkProgramGenerator.py` — Νέο αρχείο

**Τύπος:** Δημιουργία νέου αρχείου
**Αρχείο:** `backend/SparkProgramGenerator.py` (~330 γραμμές)

### Αιτία

Υλοποίηση της απαίτησης του επιβλέποντος: *«θα στέλνεται το json στο backend και το
backend θα πάρει το json και θα φτιάξει το πρόγραμμα — άρα στην ουσία θα φτιάχνω
προγράμματα προς εκτέλεση»*.

Στο αρχικό σύστημα ο χρήστης ήταν υποχρεωμένος να ανεβάσει έτοιμο εκτελέσιμο (`.jar` ή
`.py`). Δεν υπήρχε κανένας μηχανισμός παραγωγής προγράμματος. Η ενδιάμεση υλοποίηση που
είχε γίνει (ανέβασμα αρχείου JSON και χρήση του ως `executable` του job) ήταν
λανθασμένη: το `spark-submit` θα προσπαθούσε να εκτελέσει ένα αρχείο δεδομένων ως
πρόγραμμα.

### Τι κάνει

Δέχεται μια προδιαγραφή pipeline σε μορφή JSON και παράγει εκτελέσιμο πρόγραμμα PySpark
ως πηγαίο κώδικα.

**Παράδειγμα εισόδου:**

```json
{
  "name": "customer-clusters",
  "input": {"path": "/data/customers.csv", "format": "csv",
            "options": {"header": true, "inferSchema": true}},
  "steps": [{"type": "dropna"}, {"type": "filter", "condition": "age > 18"}],
  "algorithm": {"name": "kmeans", "features": ["age", "income"], "params": {"k": 4}},
  "output": {"path": "/data/out", "format": "parquet", "mode": "overwrite"}
}
```

**Παραγόμενο πρόγραμμα:**

```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


def main():
    spark = SparkSession.builder.appName('customer-clusters').getOrCreate()

    reader = spark.read.format('csv')
    reader = reader.option('header', 'true')
    reader = reader.option('inferSchema', 'true')
    df = reader.load('/data/customers.csv')

    df = df.dropna()
    df = df.filter('age > 18')

    assembler = VectorAssembler(inputCols=['age', 'income'], outputCol="features")
    data = assembler.transform(df)

    estimator = KMeans(featuresCol="features", k=4, maxIter=20, seed=1)
    model = estimator.fit(data)
    result = model.transform(data)

    output_df = result.drop("features")
    output_df.write.format('parquet').mode('overwrite').save('/data/out')

    spark.stop()


if __name__ == '__main__':
    main()
```

### Δομή του αρχείου

**Κατάλογος αλγορίθμων (`ALGORITHMS`).** Δηλωτικό dictionary όπου κάθε αλγόριθμος
περιγράφεται με: την κλάση του PySpark που χρησιμοποιεί, το module εισαγωγής της, αν
είναι supervised (απαιτεί στήλη-ετικέτα), και τις παραμέτρους του με τύπο και
προεπιλεγμένη τιμή. Υποστηρίζονται: KMeans, Logistic Regression, Linear Regression,
Decision Tree. Η προσθήκη νέου αλγορίθμου απαιτεί μόνο μια νέα εγγραφή στο dictionary,
χωρίς καμία αλλαγή στη λογική παραγωγής.

**Επικύρωση (`validate_spec`).** Ελέγχει την προδιαγραφή πριν παραχθεί οτιδήποτε:
ύπαρξη και εγκυρότητα του input path και format, εγκυρότητα των βημάτων μετασχηματισμού,
ύπαρξη του αλγορίθμου στον κατάλογο, παρουσία label όπου απαιτείται, ορθότητα τύπων των
παραμέτρων, και συμμόρφωση των ονομάτων στηλών σε ασφαλές μοτίβο. Επιστρέφει λίστα
σφαλμάτων σε αναγνώσιμη μορφή· κενή λίστα σημαίνει έγκυρη προδιαγραφή.

**Συναρτήσεις παραγωγής (`_reader_lines`, `_step_lines`, `_algorithm_lines`,
`_writer_lines`).** Καθεμία παράγει τις γραμμές κώδικα ενός τμήματος του προγράμματος:
ανάγνωση δεδομένων, μετασχηματισμοί, εκπαίδευση μοντέλου, εγγραφή αποτελέσματος.

**Κύρια συνάρτηση (`generate_program`).** Συνθέτει τα παραπάνω μέσα σε πρότυπο και
επιστρέφει ολόκληρο το πρόγραμμα ως συμβολοσειρά.

### Σχεδιαστικές επιλογές

*Ασφάλεια.* Κάθε τιμή προερχόμενη από τον χρήστη ενσωματώνεται στον παραγόμενο κώδικα
μέσω `repr()`. Έτσι δεν μπορεί να τερματίσει πρόωρα το string literal στο οποίο
βρίσκεται και να εισαγάγει αυθαίρετο κώδικα — το ανάλογο του SQL injection στο επίπεδο
της παραγωγής κώδικα.

*Επεκτασιμότητα.* Ο διαχωρισμός δεδομένων (κατάλογος αλγορίθμων) από λογική (συναρτήσεις
παραγωγής) επιτρέπει την επέκταση με νέους αλγορίθμους χωρίς τροποποίηση του υπάρχοντος
κώδικα.

*Επικύρωση πριν την παραγωγή.* Τα σφάλματα εντοπίζονται στο επίπεδο της προδιαγραφής και
επιστρέφονται συγκεντρωτικά, αντί να εμφανιστούν αργότερα ως αποτυχία εκτέλεσης στο
cluster.

### Έλεγχος

Δοκιμάστηκε τοπικά με:

- **2 έγκυρες προδιαγραφές** — μη επιβλεπόμενη (KMeans με μετασχηματισμούς `dropna`,
  `filter`, `limit` και εγγραφή σε parquet) και επιβλεπόμενη (Logistic Regression με
  label και custom παραμέτρους). Και στις δύο περιπτώσεις ο παραγόμενος κώδικας
  μεταγλωττίζεται επιτυχώς (`compile()`).
- **7 άκυρες προδιαγραφές** — κενή προδιαγραφή, μη υποστηριζόμενο format, άγνωστος
  αλγόριθμος, απουσία υποχρεωτικού label, μη έγκυρο όνομα στήλης, λανθασμένος τύπος
  παραμέτρου, άγνωστο βήμα μετασχηματισμού. Όλες εντοπίστηκαν με σαφή μηνύματα.

### Κατάσταση

Ολοκληρωμένο και ελεγμένο ως αυτόνομο module. Δεν έχει ακόμη συνδεθεί με το `app.py`.

---

## [003] 2026-07-29 — `backend/app.py` — Τροποποίηση

**Τύπος:** Τροποποίηση υπάρχοντος αρχείου
**Αρχείο:** `backend/app.py` — προσθήκη μίας γραμμής εισαγωγής και τεσσάρων νέων ορισμών

### Αιτία

Σύνδεση του `SparkProgramGenerator` (εγγραφή [002]) με το REST API, ώστε το frontend να
μπορεί να στείλει προδιαγραφή pipeline και το backend να παραγάγει, να αποθηκεύσει και
να καταχωρήσει προς εκτέλεση το αντίστοιχο πρόγραμμα.

### Αλλαγή 3.1 — Εισαγωγή module

**Πριν** (γραμμές 6-7):

```python
import DatabaseConnection as dc     # Python file managing database
import ManageOptions as mo  # Python file checking jobs
```

**Μετά:**

```python
import DatabaseConnection as dc     # Python file managing database
import ManageOptions as mo  # Python file checking jobs
import SparkProgramGenerator as spg  # Python file generating PySpark programs from a JSON spec
```

### Αλλαγή 3.2 — Νέα βοηθητική συνάρτηση `save_generated_program`

Δεν υπήρχε αντίστοιχη λειτουργία. Γράφει τον παραγόμενο κώδικα σε αρχείο `.py` μέσα στον
φάκελο του χρήστη (τον ίδιο που χρησιμοποιεί και το υπάρχον `/upload_executable`),
δημιουργώντας τον φάκελο αν δεν υπάρχει. Το όνομα αρχείου σχηματίζεται ως
`{uuid4}-{όνομα pipeline}.py` και περνά από `secure_filename`, ώστε να αποκλείεται
σύγκρουση ονομάτων και διαφυγή από τον φάκελο μέσω ειδικών χαρακτήρων ή `../`.

### Αλλαγή 3.3 — Νέα βοηθητική συνάρτηση `build_job_options`

Μετατρέπει τις παραμέτρους πόρων του αιτήματος στη δομή `options` που περιμένει η βάση
και ο executor. Διατηρεί ακριβώς τα ίδια κλειδιά με τη ροή του υπάρχοντος
`/spark-job`, ώστε ένα pipeline job να είναι δυσδιάκριτο από ένα κανονικό job για τα
υπόλοιπα τμήματα του συστήματος.

Σημείωση σχεδίασης: το κλειδί `class` ορίζεται ως κενή συμβολοσειρά. Τα προγράμματα
PySpark υποβάλλονται χωρίς `--class` (η παράμετρος αφορά μόνο JVM εφαρμογές), αλλά το
σχήμα επιλογών στη βάση το δηλώνει υποχρεωτικό. Η κενή τιμή ικανοποιεί την επικύρωση,
ενώ ο `SparkCommandGenerator` παραλείπει τις κενές τιμές κατά την κατασκευή της εντολής.

### Αλλαγή 3.4 — Νέο endpoint `POST /preview_program`

Δέχεται μια προδιαγραφή και επιστρέφει τον κώδικα που θα παραγόταν, **χωρίς** να
αποθηκεύσει αρχείο ή να δημιουργήσει job. Επιτρέπει στον χρήστη να δει το πρόγραμμα πριν
το υποβάλει. Προστατεύεται από τον υπάρχοντα decorator `authentication_required`.

### Αλλαγή 3.5 — Νέο endpoint `POST /pipeline`

Η κύρια ροή. Δέχεται σώμα της μορφής:

```json
{
  "username": "zikou@pve",
  "spec": { ... προδιαγραφή pipeline ... },
  "resources": {"instances": 2, "cores": 2, "memory": 1024, "image": "apache/spark:3.5.0"}
}
```

και εκτελεί τέσσερα βήματα με τη σειρά:

1. **Επικύρωση** της προδιαγραφής. Αν αποτύχει, επιστρέφεται `400` με τη λίστα σφαλμάτων
   και τίποτα δεν γράφεται σε δίσκο ή βάση.
2. **Παραγωγή** του προγράμματος PySpark.
3. **Αποθήκευση** του προγράμματος στον φάκελο του χρήστη.
4. **Καταχώρηση** του job στη MongoDB με `status: pending` και `executable` το μονοπάτι
   του παραγόμενου αρχείου.

Επιστρέφει το μήνυμα επιτυχίας, το όνομα και το μονοπάτι του αρχείου, καθώς και τον
πηγαίο κώδικα.

### Έλεγχος

Δοκιμάστηκε με το test client του Flask, με προσομοιωμένα (stubbed) `DatabaseConnection`,
`ManageOptions` και `requests`, ώστε να μη χρειάζεται ούτε MongoDB ούτε Proxmox:

| # | Σενάριο | Αναμενόμενο | Αποτέλεσμα |
|---|---------|-------------|------------|
| 1 | `/preview_program` με έγκυρη προδιαγραφή | `200` + κώδικας | ✔ |
| 2 | `/preview_program` με άγνωστο αλγόριθμο | `400` + λίστα σφαλμάτων | ✔ |
| 3 | `/pipeline` με έγκυρη προδιαγραφή | `200`, αρχείο στον δίσκο, job στη βάση | ✔ |
| 4 | `/pipeline` χωρίς `username` | `400` | ✔ |
| 5 | `/pipeline` με απουσία υποχρεωτικού `label` | `400` | ✔ |

Επιπλέον, ελέγχθηκε η συμβατότητα των παραγόμενων `options` με το πραγματικό σχήμα
επιλογών (`database/Kubernetes.Options.json`) μέσω του `SparkCommandGenerator`: η
επικύρωση περνά χωρίς σφάλματα και παράγεται η εντολή:

```
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name customerclusters \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=1024 \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.driver.node.selector.kubernetes.io/hostname=k8s-1 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  /KubernetesAPI/backend/uploads/zikou/abc-customer_clusters.py
```

Επιβεβαιώνεται ότι το `--class` παραλείπεται σωστά και ότι το τελευταίο όρισμα είναι το
παραγόμενο πρόγραμμα.

### Κατάσταση

Ολοκληρωμένο και ελεγμένο. Εκκρεμεί η προσαρμογή του frontend ώστε να καλεί τα νέα
endpoints.

---

## [004] 2026-07-29 — `frontend/.../pipeline-creation` — Πλήρης αναμόρφωση

**Τύπος:** Αντικατάσταση περιεχομένου τριών υπαρχόντων αρχείων
**Αρχεία:**

- `frontend/src/app/pages/pipeline-creation/pipeline-creation.component.ts`
- `frontend/src/app/pages/pipeline-creation/pipeline-creation.component.html`
- `frontend/src/app/pages/pipeline-creation/pipeline-creation.component.css` (ήταν κενό)

### Αιτία

Διευκρίνιση του επιβλέποντος (μήνυμα 16/07): στην ερώτηση *«εγώ θα κατασκευάζω το json
δηλαδή; δεν θα το δίνει ο χρήστης σαν είσοδο;»* η απάντηση ήταν **ναι**, με τη
διατύπωση *«το json να το κατασκευάζεις μέσω μιας φόρμας στο frontend»*.

Η προηγούμενη μορφή της σελίδας ζητούσε από τον χρήστη να **ανεβάσει** αρχείο JSON και
χρησιμοποιούσε το μονοπάτι του ως `executable` του job. Αυτό ήταν λανθασμένο σε δύο
επίπεδα: αντιβαίνει στην απαίτηση (ο χρήστης δεν πρέπει να δίνει το JSON), και τεχνικά
το `spark-submit` θα προσπαθούσε να εκτελέσει αρχείο δεδομένων ως πρόγραμμα.

### Τι αφαιρέθηκε

| Στοιχείο | Σχόλιο |
|----------|--------|
| `<input type="file" accept=".json">` | Το ανέβασμα αρχείου JSON |
| `onFileSelected()`, `selectedFile`, `fileName`, `noFileError` | Η λογική διαχείρισης του αρχείου |
| Κλήση στο `/upload_executable` | Δεν ανεβαίνει πλέον αρχείο |
| Πεδίο `class` ως ελεύθερο κείμενο | Ο αλγόριθμος επιλέγεται πλέον από λίστα και δεν εκφράζεται ως Java class name |
| Σταθερό `executable: 'local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar'` | Ήταν χειροκίνητα καρφωμένο· τώρα το εκτελέσιμο παράγεται |

### Τι προστέθηκε

Η φόρμα κατασκευάζει πλέον την προδιαγραφή JSON και είναι οργανωμένη σε πέντε ενότητες:

1. **General** — όνομα του pipeline.
2. **Input data** — μονοπάτι δεδομένων, format (`csv`/`json`/`parquet`/`orc`/`text`), και
   για CSV οι επιλογές `header` και `inferSchema`.
3. **Transformations** — δυναμική λίστα βημάτων (`FormArray`) με κουμπιά προσθήκης και
   αφαίρεσης. Οι διαθέσιμοι τύποι είναι `select`, `filter`, `dropna`, `limit`, και το
   πεδίο που εμφανίζεται προσαρμόζεται στον επιλεγμένο τύπο.
4. **Algorithm** — επιλογή αλγορίθμου, στήλες χαρακτηριστικών, στήλη ετικέτας (εμφανίζεται
   μόνο για επιβλεπόμενους αλγορίθμους), και οι παράμετροι του αλγορίθμου.
5. **Output & Cluster resources** — προαιρετικό μονοπάτι εξόδου με format και write mode,
   και οι πόροι εκτέλεσης (instances, cores, memory, container image).

Δύο ενέργειες αντί για μία:

- **Preview generated program** → καλεί `POST /preview_program` και εμφανίζει τον κώδικα
  που θα παραχθεί, χωρίς να αποθηκευτεί τίποτα.
- **Create pipeline** → καλεί `POST /pipeline`, δημιουργεί το job και εμφανίζει τον
  τελικό κώδικα μαζί με το όνομα του παραγόμενου αρχείου.

### Σχεδιαστικές επιλογές

*Κατοπτρισμός του καταλόγου αλγορίθμων.* Ο πίνακας `ALGORITHMS` του component
αντικατοπτρίζει τον αντίστοιχο κατάλογο του `SparkProgramGenerator.py`. Έτσι η φόρμα
προσφέρει μόνο συνδυασμούς που το backend μπορεί όντως να παραγάγει, και τα πεδία
παραμέτρων δημιουργούνται δυναμικά (`rebuildParamControls`) όταν αλλάξει ο αλγόριθμος.
Το πεδίο ετικέτας γίνεται υποχρεωτικό ή προαιρετικό ανάλογα με τον αλγόριθμο.

*Εμφάνιση σφαλμάτων επικύρωσης.* Όταν το backend απορρίπτει μια προδιαγραφή, η λίστα
σφαλμάτων που επιστρέφει εμφανίζεται αναλυτικά στον χρήστη, αντί για ένα γενικό
μήνυμα αποτυχίας.

*Αντικατάσταση των `alert()`.* Η προηγούμενη έκδοση χρησιμοποιούσε native `alert()`, που
διακόπτει τη ροή του χρήστη. Πλέον η ανάδραση εμφανίζεται μέσα στη σελίδα.

*Στήλες ως κείμενο χωρισμένο με κόμμα.* Οι στήλες δίνονται ως `age, income` και
μετατρέπονται σε πίνακα από τη `splitColumns()`.

### Παράδειγμα παραγόμενης προδιαγραφής

Η φόρμα με τιμές `customer-clusters`, `/data/customers.csv`, CSV με header, ένα βήμα
`dropna`, ένα βήμα `filter` με `age > 18`, αλγόριθμο K-Means με στήλες `age, income` και
`k = 4`, και έξοδο σε `/data/out` παράγει:

```json
{
  "name": "customer-clusters",
  "input": {"path": "/data/customers.csv", "format": "csv",
            "options": {"header": true, "inferSchema": true}},
  "steps": [{"type": "dropna"}, {"type": "filter", "condition": "age > 18"}],
  "algorithm": {"name": "kmeans", "features": ["age", "income"],
                "params": {"k": 4, "maxIter": 20, "seed": 1}},
  "output": {"path": "/data/out", "format": "parquet", "mode": "overwrite"}
}
```

### Έλεγχος

- `tsc -p tsconfig.app.json --noEmit` → **0 σφάλματα** (11 δευτερόλεπτα).
- `ngc -p tsconfig.app.json --noEmit` → **0 σφάλματα** (21 δευτερόλεπτα). Το `ngc`
  ελέγχει επιπλέον τα πρότυπα (templates) με `strictTemplates: true`, που είναι
  ενεργοποιημένο στο `tsconfig.json` του έργου· επομένως επιβεβαιώνεται και η ορθότητα
  του HTML ως προς τους τύπους.

### Κατάσταση

Ολοκληρωμένο σε επίπεδο μεταγλώττισης. Εκκρεμεί δοκιμή στον browser με το σύστημα σε
λειτουργία (απαιτεί ολοκλήρωση του build των containers).

---

## [005] 2026-07-29 — Επαλήθευση στον browser — Επιτυχής

**Τύπος:** Έλεγχος λειτουργίας (καμία αλλαγή κώδικα)
**Περιβάλλον:** `docker compose up --build mongo kubernetes-api kubernetes-ui`
(ο executor δεν συμμετέχει: η νέα λειτουργία ολοκληρώνεται μεταξύ frontend, backend και
MongoDB)

### Σενάρια και αποτελέσματα

| # | Σενάριο | Αναμενόμενο | Αποτέλεσμα |
|---|---------|-------------|------------|
| 1 | K-Means χωρίς έξοδο, χωρίς μετασχηματισμούς. Preview και δημιουργία. | Παραγόμενος κώδικας που τελειώνει σε `result.show(...)`· job στη βάση με `executable` σε `.py` | ✔ |
| 2 | Logistic Regression με τρία βήματα (`dropna`, `filter age > 18`, `limit 5000`), label `churned`, έξοδος σε parquet | Τα τρία βήματα με τη σειρά, `labelCol='churned'`, εγγραφή στο μονοπάτι εξόδου· δυναμική εμφάνιση του πεδίου ετικέτας και αλλαγή των παραμέτρων κατά την επιλογή αλγορίθμου | ✔ |
| 3α | Απουσία υποχρεωτικής ετικέτας | Απόρριψη από το frontend πριν την αποστολή | ✔ |
| 3β | Όνομα στήλης `income; DROP TABLE users` | Απόρριψη από το **backend** με συγκεκριμένο μήνυμα επικύρωσης | ✔ |
| 3γ | Τερματισμός του `api` container | Μήνυμα αδυναμίας επικοινωνίας με το backend | ✔ |

### Συμπέρασμα

Η ροή *φόρμα → JSON → backend → παραγωγή προγράμματος PySpark → καταχώρηση job*
επιβεβαιώθηκε από άκρο σε άκρο, με τα δύο επίπεδα επικύρωσης (frontend και backend) να
λειτουργούν ανεξάρτητα.

Δεν έχει επιβεβαιωθεί ακόμη η **εκτέλεση** του παραγόμενου προγράμματος, καθώς απαιτεί
τον executor και πρόσβαση σε Kubernetes cluster.

---

## [006] 2026-07-29 — Εντοπισμός σφαλμάτων στον μηχανισμό επικύρωσης επιλογών

**Τύπος:** Διερεύνηση (καμία αλλαγή κώδικα σε αυτή την εγγραφή)
**Αρχεία που εξετάστηκαν:** `database/mongo-init.sh`, `backend/DatabaseConnection.py`,
`executor/DatabaseConnection.py`, `backend/InsertOptions.py`, `backend/ManageOptions.py`

### Πώς προέκυψε

Κατά τον έλεγχο του `SparkCommandGenerator` παρατηρήθηκε ότι η `validate_options()`
επέστρεφε πάντα κενή λίστα σφαλμάτων, ακόμη και για προφανώς άκυρες τιμές. Η διερεύνηση
έδειξε ότι η αιτία δεν ήταν στη λογική επικύρωσης, αλλά στο ότι το σύνολο των κανόνων
βάσει των οποίων γίνεται η επικύρωση **δεν φορτώνεται ποτέ**.

### Παρατηρούμενη κατάσταση της βάσης

```
> db.getSiblingDB('Kubernetes').getCollectionNames()
[ 'Jobs' ]

> db.getSiblingDB('Kubernetes').getCollection('options').countDocuments()
0
```

Το collection με τους ορισμούς των επιλογών δεν υπάρχει καθόλου.

### Εντοπισμένα σφάλματα

**Σφάλμα 1 — Ελληνικό όμικρον στο όνομα collection.**
`database/mongo-init.sh`, γραμμή 182:

```
db.οptions.insertMany(db_options);
```

Ο πρώτος χαρακτήρας μετά το `db.` είναι **ελληνικό όμικρον** (U+03BF), όχι λατινικό `o`.
Επιβεβαιώθηκε με έλεγχο των bytes: `64 62 2e ce bf 70 74 69 6f 6e 73` — δηλαδή `db.` +
`ce bf` (η κωδικοποίηση UTF-8 του ελληνικού `ο`) + `ptions`. Οπτικά είναι πανομοιότυπο με
το λατινικό, αλλά για τη MongoDB πρόκειται για εντελώς διαφορετικό όνομα.

**Σφάλμα 2 — Κλήση `mongosh` χωρίς διαπιστευτήρια.**
`database/mongo-init.sh`, γραμμή 3: `mongosh <<EOF`. Η επίσημη εικόνα της MongoDB
δημιουργεί τον χρήστη root πριν εκτελέσει τα σενάρια αρχικοποίησης, οπότε η σύνδεση
χωρίς `-u`/`-p` απορρίπτεται με `MongoServerError: ... requires authentication`. Σε
συνδυασμό με το `set -e` της πρώτης γραμμής, το σενάριο τερματίζει πριν εισαχθεί
οτιδήποτε.

**Σφάλμα 3 — Ασυνέπεια ονομάτων μέσα στο ίδιο αρχείο.**
`backend/DatabaseConnection.py`: η `add_option()` (γραμμή 129) γράφει στο `'Options'`
(κεφαλαίο O), ενώ οι `retrieve_options()` (γραμμή 135) και `find_option_by_name()`
(γραμμή 146) διαβάζουν από το `'options'` (πεζό). Πρόκειται για δύο διαφορετικά
collections.

**Σφάλμα 4 — Ασυνέπεια ονομάτων μεταξύ υπηρεσιών.**
`executor/DatabaseConnection.py` (γραμμές 135, 145) διαβάζει από το `'Options'`
(κεφαλαίο), ενώ το backend διαβάζει από το `'options'` (πεζό). Υπάρχουν συνολικά **τρεις**
παραλλαγές του ίδιου ονόματος στον κώδικα, καμία από τις οποίες δεν ταυτίζεται με αυτή
που επιχειρεί να δημιουργήσει το σενάριο αρχικοποίησης.

**Σφάλμα 5 — Δομικό: τα σενάρια αρχικοποίησης δεν επανεκτελούνται.**
Στο `docker-compose.yml` ο τόμος `mongo_vol` δηλώνεται `external: true`. Η MongoDB
εκτελεί τα σενάρια του `docker-entrypoint-initdb.d` **μόνο** όταν ο κατάλογος δεδομένων
είναι κενός. Σε έναν τόμο που έχει ήδη χρησιμοποιηθεί, ακόμη και αν διορθωθούν τα
Σφάλματα 1 και 2, το σενάριο δεν θα εκτελεστεί ποτέ ξανά.

### Συνέπειες

Το collection των ορισμών είναι κενό, οπότε η `retrieve_options()` επιστρέφει κενή λίστα.
Επειδή ο έλεγχος υποχρεωτικών πεδίων εκφράζεται ως «για κάθε ορισμό, έλεγξε αν το
αντίστοιχο πεδίο υπάρχει», με μηδέν ορισμούς **δεν ελέγχεται τίποτα** και η επικύρωση
περνά πάντα. Αυτό αφορά:

- `backend/ManageOptions.check_spark_job()` — και το endpoint `/check_job` που το καλεί.
  Στην περίπτωση αυτή η `find_option_by_name()` επιστρέφει `None` και η επόμενη πρόσβαση
  `db_option['values']` προκαλεί `TypeError`.
- `executor/SparkCommandGenerator.validate_options()` — επιστρέφει «καθαρό» αποτέλεσμα
  όχι επειδή οι επιλογές είναι έγκυρες, αλλά επειδή δεν υπάρχει κανένας κανόνας να τις
  κρίνει.

Η επικύρωση της νέας ροής pipeline (`SparkProgramGenerator.validate_spec`) **δεν
επηρεάζεται**, καθώς οι κανόνες της είναι δηλωμένοι στον κώδικα και δεν εξαρτώνται από τη
βάση. Αυτό εξηγεί γιατί οι έλεγχοι της εγγραφής [005] πέτυχαν κανονικά.

### Κατάσταση

Εντοπισμένο και τεκμηριωμένο. Η διόρθωση δεν έχει γίνει ακόμη.

---

## [007] 2026-07-29 — Διόρθωση των σφαλμάτων της εγγραφής [006]

**Τύπος:** Τροποποίηση τεσσάρων υπαρχόντων αρχείων
**Αρχεία:** `database/mongo-init.sh`, `backend/DatabaseConnection.py`,
`executor/DatabaseConnection.py`, `docker-compose.yml`, `backend/app.py`

### Αλλαγή 7.1 — `database/mongo-init.sh`: ελληνικό όμικρον (Σφάλμα 1)

**Πριν** (γραμμή 182): `db.οptions.insertMany(db_options);` (πρώτος χαρακτήρας U+03BF)
**Μετά:** `db.options.insertMany(db_options);` (πρώτος χαρακτήρας U+006F)

Επιβεβαιώθηκε με προγραμματιστικό έλεγχο ότι στη νέα μορφή του αρχείου δεν υπάρχει
κανένας χαρακτήρας εκτός ASCII στις γραμμές που αφορούν τη MongoDB.

### Αλλαγή 7.2 — `database/mongo-init.sh`: διαπιστευτήρια (Σφάλμα 2)

**Πριν** (γραμμή 3):

```bash
mongosh <<EOF
```

**Μετά:**

```bash
mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin <<EOF
```

Οι δύο μεταβλητές παρέχονται από την επίσημη εικόνα της MongoDB στα σενάρια
αρχικοποίησης. Η βάση πιστοποίησης είναι η `admin`, όπου δημιουργείται ο χρήστης root.

### Αλλαγή 7.3 — `backend/DatabaseConnection.py`: ενοποίηση ονόματος (Σφάλμα 3)

Προστέθηκε σταθερά ως μοναδική πηγή αλήθειας:

```python
OPTIONS_COLLECTION = 'options'
OPTIONS_SEED_FILE = os.environ.get('OPTIONS_SEED_FILE', './database/Kubernetes.Options.json')
```

Οι τρεις συναρτήσεις που αναφέρονταν στο collection με κυριολεκτικές συμβολοσειρές
χρησιμοποιούν πλέον τη σταθερά:

| Συνάρτηση | Πριν | Μετά |
|-----------|------|------|
| `add_option()` | `database_connection('Options')` | `database_connection(OPTIONS_COLLECTION)` |
| `retrieve_options()` | `database_connection('options')` | `database_connection(OPTIONS_COLLECTION)` |
| `find_option_by_name()` | `database_connection('options')` | `database_connection(OPTIONS_COLLECTION)` |

### Αλλαγή 7.4 — `executor/DatabaseConnection.py`: ενοποίηση ονόματος (Σφάλμα 4)

Προστέθηκε η αντίστοιχη σταθερά `OPTIONS_COLLECTION = 'options'` και οι τρεις
συναρτήσεις (`add_option`, `retrieve_options`, `find_option_by_name`) άλλαξαν από
`database_connection('Options')` σε `database_connection(OPTIONS_COLLECTION)`.

Ελέγχθηκε με αναζήτηση σε όλα τα αρχεία Python ότι δεν απομένει καμία κυριολεκτική
αναφορά σε `'Options'` ή `'options'` ως όνομα collection.

### Αλλαγή 7.5 — `backend/DatabaseConnection.py`: νέα συνάρτηση `seed_options()` (Σφάλμα 5)

Το δομικό σφάλμα δεν λύνεται με διόρθωση του σεναρίου αρχικοποίησης, γιατί σε έναν τόμο
που έχει ήδη χρησιμοποιηθεί το σενάριο δεν εκτελείται ποτέ ξανά. Προστέθηκε συνάρτηση που
φορτώνει τους ορισμούς όταν το collection είναι κενό:

- Αν το collection έχει έγγραφα, δεν κάνει τίποτα (ιδιότητα idempotency).
- Αν είναι κενό, διαβάζει το `database/Kubernetes.Options.json` και εισάγει τους ορισμούς.
- Αν το αρχείο απουσιάζει, τυπώνει προειδοποίηση χωρίς να διακόψει την εκκίνηση.

### Αλλαγή 7.6 — `backend/app.py`: κλήση κατά την εκκίνηση

**Πριν:**

```python
''' MAIN '''
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
```

**Μετά:**

```python
''' MAIN '''
try:
    dc.seed_options()
except Exception as seed_error:
    print(f'\nWARNING: could not seed option definitions: {seed_error}\n')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
```

Η κλήση βρίσκεται εκτός του ελέγχου `__name__ == '__main__'`, ώστε να εκτελείται και όταν
η εφαρμογή σερβίρεται από εξωτερικό WSGI server. Είναι σε `try`/`except` ώστε αδυναμία
φόρτωσης των ορισμών να μην εμποδίζει την εκκίνηση του API.

### Αλλαγή 7.7 — `docker-compose.yml`: πρόσβαση στο αρχείο ορισμών

**Πριν:**

```yaml
    volumes:
      - uploads_vol:/KubernetesAPI/backend/uploads
```

**Μετά:**

```yaml
    volumes:
      - uploads_vol:/KubernetesAPI/backend/uploads
      # Read-only: the option definitions the API seeds into MongoDB on start-up
      - ./database:/KubernetesAPI/database:ro
```

Το `Dockerfile` του backend αντιγράφει μόνο τα περιεχόμενα του φακέλου `backend/`, οπότε
το αρχείο ορισμών δεν υπήρχε μέσα στην εικόνα. Προσαρτάται μόνο για ανάγνωση, ώστε να
παραμένει μία πηγή αλήθειας και να μην δημιουργηθεί τέταρτο αντίγραφο του σχήματος.

### Έλεγχος

**Συντακτικός έλεγχος.** Τα πέντε αρχεία Python περνούν `ast.parse` χωρίς σφάλμα. Το
`docker-compose.yml` αναλύεται σωστά ως YAML και ο τόμος εμφανίζεται στη σωστή υπηρεσία.

**Λειτουργία `seed_options()`.** Δοκιμάστηκε με προσομοιωμένο collection:

| # | Σενάριο | Αποτέλεσμα |
|---|---------|------------|
| 1 | Κενό collection | Εισήχθησαν 12 ορισμοί στο collection `options` |
| 2 | Δεύτερη κλήση | Δεν εισήχθη τίποτα, παρέμειναν 12 |
| 3 | Απουσία αρχείου ορισμών | Προειδοποίηση, επιστροφή 0, καμία εξαίρεση |

**Απόδειξη ότι η επικύρωση απέκτησε πλέον ισχύ.** Με τους 12 ορισμούς φορτωμένους, η
`SparkCommandGenerator.validate_options()` εντοπίζει σφάλματα που πριν περνούσαν
απαρατήρητα:

| Είσοδος | Αποτέλεσμα |
|---------|------------|
| Έγκυρο job | `[]` |
| `deploy-mode = standalone` | `out of allowed range ['cluster', 'client']` |
| `spark.executor.cores = 99` | `out of allowed range ['1', '8']` |
| `spark.executor.memory = 100` | `out of allowed range ['512', '10240']` |
| Απουσία `master` | `missing required option` |

Πριν τη διόρθωση, και οι πέντε περιπτώσεις επέστρεφαν `[]`.

### Κατάσταση

Ολοκληρωμένο και ελεγμένο τοπικά. Εκκρεμεί επιβεβαίωση στο ζωντανό σύστημα (επανεκκίνηση
του `api` container και έλεγχος ότι το collection `options` περιέχει 12 έγγραφα).

---

## [008] 2026-07-29 — Επιβεβαίωση της διόρθωσης [007] στο ζωντανό σύστημα

**Τύπος:** Έλεγχος λειτουργίας (καμία αλλαγή κώδικα)

### Διαδικασία

```bash
docker compose up --build mongo kubernetes-api kubernetes-ui
docker logs api | findstr /i "Seeded WARNING options"
```

### Αποτέλεσμα

```
/KubernetesAPI/ManageOptions.py is running...
Seeded 12 option definitions into "options"
```

### Συμπέρασμα

Η συνάρτηση `seed_options()` εκτελέστηκε κατά την εκκίνηση του API, βρήκε το collection
κενό και εισήγαγε τους 12 ορισμούς επιλογών. Δεν εμφανίστηκε η προειδοποίηση απουσίας
αρχείου, επομένως η προσάρτηση του φακέλου `database` (Αλλαγή 7.7) λειτουργεί.

Η αλυσίδα επικύρωσης που ήταν ανενεργή από την αρχή του έργου (εγγραφή [006]) είναι πλέον
λειτουργική: `mongo-init.sh` και `seed_options()` γεμίζουν το ίδιο collection, και τα δύο
`DatabaseConnection.py` το διαβάζουν με το ίδιο όνομα.

---

## [009] 2026-07-31 — `executor/Dockerfile` — Αναθεώρηση με βάση επίσημη εικόνα Spark

**Τύπος:** Αντικατάσταση περιεχομένου υπάρχοντος αρχείου
**Αρχεία:** `executor/Dockerfile`, `executor/kubectl_config.sh`

### Αιτία

Το build του executor δεν ολοκληρωνόταν σε αποδεκτό χρόνο (πάνω από 90 λεπτά χωρίς
τερματισμό), γεγονός που εμπόδιζε κάθε δοκιμή εκτέλεσης. Η αναθεώρηση της εγγραφής [001]
βελτίωσε το βήμα εγκατάστασης πακέτων, αλλά παρέμεναν δύο βαριές λήψεις: το JDK και το
συμπιεσμένο αρχείο του Spark (~200 MB από το `archive.apache.org`).

Κατά την ανάλυση εντοπίστηκαν επιπλέον τρία λογικά προβλήματα.

### Εντοπισμένα προβλήματα

**Πρόβλημα Α — Ασυμφωνία εκδόσεων Spark.** Το `Dockerfile` εγκαθιστούσε **Spark 3.0.2**
(`ENV SPARK_VERSION=3.0.2`), ενώ τα jobs ζητούν εικόνα εκτελεστών
`spark.kubernetes.container.image = apache/spark:3.5.0`. Ο υποβάλλων και το cluster
εκτελούσαν διαφορετικές εκδόσεις.

**Πρόβλημα Β — Λανθασμένη διαδρομή του `spark-submit`.** Το Spark εγκαθίστατο στο
`/KubernetesAPI/executor/opt/spark` (`ENV SPARK_HOME`), άρα το εκτελέσιμο βρισκόταν στο
`/KubernetesAPI/executor/opt/spark/bin/spark-submit`. Όμως η προεπιλεγμένη τιμή της
επιλογής `spark_path` στο `database/Kubernetes.Options.json` είναι
`/opt/spark/bin/spark-submit`, διαδρομή που δεν υπήρχε στο container.

**Πρόβλημα Γ — Η ρύθμιση του kubectl δεν διαβαζόταν ποτέ.** Το `kubectl_config.sh`
έγραφε στο `~/.kube/config-kubectl`, ενώ το `kubectl` διαβάζει το `~/.kube/config` (ή
όποιο αρχείο ορίζει η μεταβλητή `KUBECONFIG`, που δεν οριζόταν πουθενά). Επιπλέον, η
γραμμή `RUN kubectl config set-cluster KubernetesAPI` που προηγούνταν δημιουργούσε ένα
`~/.kube/config` με κενή καταχώριση cluster, το οποίο ήταν και το μόνο που χρησιμοποιούσε
τελικά το `kubectl`.

### Επιλογή base image

Επιβεβαιώθηκε μέσω του Docker Hub API ότι οι ετικέτες `apache/spark:3.5.0`,
`apache/spark:3.5.0-python3` και `apache/spark:3.5.0-scala2.12-java11-python3-ubuntu`
αντιστοιχούν στο **ίδιο** image (κοινό digest `sha256:0ed5154e...`, μέγεθος 534 MB). Η
εικόνα περιέχει ήδη Java 11, Spark 3.5.0 εγκατεστημένο στο `/opt/spark`, και python3.

Η χρήση της εξαλείφει τα δύο βαριά βήματα του build και λύνει ταυτόχρονα τα Προβλήματα Α
και Β: η έκδοση ταυτίζεται με αυτή των εκτελεστών, και το `spark-submit` βρίσκεται
ακριβώς στη διαδρομή που δηλώνει το σχήμα επιλογών.

### Πριν (δομή)

```dockerfile
FROM ubuntu:22.04
ENV SPARK_VERSION=3.0.2
ENV SPARK_HOME=/KubernetesAPI/executor/opt/spark
RUN apt-get update && apt-get install -q -y --no-install-recommends openjdk-8-jdk-headless ...
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/..." \
    && mkdir -p /KubernetesAPI/executor/opt/spark \
    && tar -xf apache-spark.tgz -C /KubernetesAPI/executor/opt/spark --strip-components=1 \
    && rm apache-spark.tgz
RUN apt-get update && apt-get install -y curl
RUN apt install sudo
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
RUN curl -LO ".../kubectl.sha256"
RUN echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
RUN sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
RUN kubectl version --client
COPY ./* .
RUN python3 -m pip install -r requirements2.txt
RUN kubectl config set-cluster KubernetesAPI
RUN ./kubectl_config.sh
CMD ["python3", "JobExecutor.py"]
```

### Μετά (δομή)

```dockerfile
FROM apache/spark:3.5.0
USER root
ENV PYTHONHASHSEED=1
ENV DEBIAN_FRONTEND=noninteractive
# (οι μεταβλητές πιστοποιητικών και SERVER παραμένουν όπως ήταν)
WORKDIR /KubernetesAPI/executor
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*
RUN KUBECTL_VERSION="$(curl -fsSL https://dl.k8s.io/release/stable.txt)" && \
    curl -fsSLO ".../kubectl" && curl -fsSLO ".../kubectl.sha256" && \
    echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm -f kubectl kubectl.sha256 && kubectl version --client
COPY ./* .
RUN python3 -m pip install --no-cache-dir -r requirements2.txt
RUN mkdir -p /root/.kube && bash ./kubectl_config.sh && kubectl config get-contexts
CMD ["python3", "JobExecutor.py"]
```

### Ανάλυση των αλλαγών

| # | Αλλαγή | Σκοπός |
|---|--------|--------|
| 1 | `FROM ubuntu:22.04` → `FROM apache/spark:3.5.0` | Java, Spark και python3 έτοιμα· καταργείται η λήψη JDK και του αρχείου Spark |
| 2 | Προσθήκη `USER root` | Η επίσημη εικόνα τρέχει ως μη προνομιούχος χρήστης `spark`· η εγκατάσταση πακέτων απαιτεί root |
| 3 | Αφαίρεση `SPARK_VERSION`, `HADOOP_VERSION`, `SPARK_HOME` | Ορίζονται πλέον από την ίδια την εικόνα (`SPARK_HOME=/opt/spark`) |
| 4 | Ενοποίηση επτά `RUN` σε τρία | Κάθε `RUN` δημιουργεί ξεχωριστό layer· λιγότερα layers σημαίνουν μικρότερη εικόνα και γρηγορότερο build |
| 5 | Αφαίρεση `apt install sudo` | Περιττό: ο χρήστης είναι ήδη root κατά το build |
| 6 | `curl -fsSL` αντί `curl -L -s` | Το `-f` κάνει το `curl` να αποτυγχάνει σε σφάλμα HTTP αντί να αποθηκεύει τη σελίδα σφάλματος ως εκτελέσιμο |
| 7 | Διαγραφή των `kubectl`, `kubectl.sha256` μετά την εγκατάσταση | Δεν παραμένουν αντίγραφα στο image |
| 8 | Αφαίρεση `RUN kubectl config set-cluster KubernetesAPI` | Δημιουργούσε ημιτελές `~/.kube/config` που επισκίαζε την πραγματική ρύθμιση |
| 9 | `RUN ./kubectl_config.sh` → `RUN mkdir -p /root/.kube && bash ./kubectl_config.sh` | Ο κατάλογος πρέπει να υπάρχει πριν γράψει το `tee`· η ρητή κλήση `bash` δεν εξαρτάται από το δικαίωμα εκτέλεσης του αρχείου, που δεν διατηρείται πάντα σε Windows |
| 10 | Προσθήκη `kubectl config get-contexts` | Επαληθεύει κατά το build ότι η ρύθμιση διαβάζεται σωστά· αν αποτύχει, το build σταματά αμέσως αντί να αποτύχει αργότερα κατά την εκτέλεση |

### Αλλαγή στο `executor/kubectl_config.sh` (Πρόβλημα Γ)

**Πριν** (γραμμή 3): `tee ~/.kube/config-kubectl > /dev/null <<EOT`
**Μετά:** `tee ~/.kube/config > /dev/null <<EOT`

### Παρατήρηση ασφαλείας (δεν αντιμετωπίστηκε)

Το `Dockerfile` περιέχει σε καθαρή μορφή, ως τιμές `ENV`, το πιστοποιητικό πελάτη και το
**ιδιωτικό κλειδί** πρόσβασης στο Kubernetes cluster (`CLIENT_KEY_DATA`). Οι τιμές αυτές
βρίσκονται στο αποθετήριο και ενσωματώνονται στο image, όπου είναι αναγνώσιμες από
οποιονδήποτε έχει πρόσβαση σε αυτό. Η ενδεδειγμένη πρακτική είναι η μεταφορά τους στο
αρχείο `.env` (που εξαιρείται από το git) και η διοχέτευσή τους μέσω του
`docker-compose.yml`. Δεν έγινε αλλαγή στην παρούσα φάση ώστε να μη διακοπεί η υπάρχουσα
ρύθμιση· καταγράφεται ως εκκρεμότητα.

### Κατάσταση

Γραμμένο, δεν έχει ακόμη εκτελεστεί build. Εκκρεμεί επιβεβαίωση του χρόνου build και της
εκκίνησης του container.

---

## [010] 2026-08-01 — Πρώτη εκτέλεση του executor — Μερική επιτυχία

**Τύπος:** Έλεγχος λειτουργίας (καμία αλλαγή κώδικα)

### Τι πέτυχε

Για πρώτη φορά στην πορεία της εργασίας ο executor χτίστηκε και εκτελέστηκε από άκρο σε
άκρο:

1. Το build ολοκληρώθηκε (εγγραφή [009]).
2. Το container ξεκίνησε και συνδέθηκε στη MongoDB.
3. Ο βρόχος `job_selector()` εντόπισε τα εκκρεμή jobs.
4. Ο `SparkCommandGenerator` επικύρωσε τις επιλογές και παρήγαγε σωστή εντολή:

```
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name test-pipeline \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=1 \
  ...
  local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar
```

5. Το `spark-submit` **εκτελέστηκε πραγματικά**: η JVM ξεκίνησε, το Spark 3.5.0
   αρχικοποιήθηκε και επιχείρησε να επικοινωνήσει με το Kubernetes API. Επιβεβαιώνεται
   ότι η αλλαγή base image της εγγραφής [009] απέδωσε — Java και Spark λειτουργούν, και
   το `spark-submit` βρέθηκε στη διαδρομή `/opt/spark/bin/spark-submit` που δηλώνει το
   σχήμα (Πρόβλημα Β της [009] επιλύθηκε).

### Τι απέτυχε

```
Caused by: java.net.UnknownHostException: kubernetes.default.svc
```

Ο βρόχος ολοκλήρωσε σωστά τον κύκλο του: κατέγραψε log, σημείωσε το job ως `failed` με
`priority: 2`, και προχώρησε στο επόμενο.

### Ανάλυση της αποτυχίας

Η τιμή `master = k8s://https://kubernetes.default.svc` είναι το **εσωτερικό** όνομα DNS
του Kubernetes API. Επιλύεται μόνο από διεργασίες που εκτελούνται **μέσα** στο cluster,
όπου το DNS του Kubernetes είναι διαθέσιμο. Ο executor όμως τρέχει ως container του
Docker στον υπολογιστή του χρήστη, εκτός cluster, οπότε το όνομα δεν αντιστοιχεί σε
καμία διεύθυνση.

Η σωστή τιμή είναι η εξωτερική διεύθυνση του API server, δηλαδή `k8s://` ακολουθούμενο
από την τιμή της μεταβλητής `SERVER` του `Dockerfile` (`https://192.168.0.201:16443`).
Το ίδιο δείχνει και το σχολιασμένο παράδειγμα στο `executor/JobExecutor.py`, γραμμή 62.

Η λανθασμένη τιμή προέρχεται από τον κώδικα του component `pipeline-creation` και
αντιγράφηκε στη συνέχεια στη συνάρτηση `build_job_options` του `app.py` (εγγραφή [003]).

### Λοιπά εμπόδια που εντοπίστηκαν στην ίδια εκτέλεση

**Εμπόδιο 1 — Ο τόμος των αρχείων δεν είναι προσαρτημένος στον executor.** Στο
`docker-compose.yml` η υπηρεσία `kubernetes-executor` δεν δηλώνει κανένα `volumes`. Ο
τόμος `uploads_vol`, όπου αποθηκεύονται τα παραγόμενα προγράμματα, προσαρτάται μόνο στην
υπηρεσία `kubernetes-api`. Επομένως ο executor δεν βλέπει τα αρχεία που καλείται να
υποβάλει. Το πρόβλημα είναι προϋπάρχον και αφορούσε εξίσου τα αρχεία `.jar` που ανέβαζε
ο χρήστης στην αρχική ροή.

**Εμπόδιο 2 — Σχετική διαδρομή στο `executable`.** Τα jobs αποθηκεύουν διαδρομή της
μορφής `./backend/uploads/<user>/<αρχείο>.py`, η οποία εξαρτάται από τον τρέχοντα
κατάλογο. Στο container του API ο κατάλογος είναι `/KubernetesAPI` και η διαδρομή
επιλύεται σωστά· στον executor είναι `/KubernetesAPI/executor` και η επίλυση αποτυγχάνει.

**Εμπόδιο 3 — Υποβολή τοπικού αρχείου σε cluster mode.** Ακόμη και με τα δύο παραπάνω
λυμένα, η υποβολή τοπικού αρχείου `.py` με `--deploy-mode cluster` απαιτεί από το Spark
τη ρύθμιση `spark.kubernetes.file.upload.path` (κοινόχρηστο σημείο αποθήκευσης όπου το
`spark-submit` ανεβάζει το αρχείο ώστε να το κατεβάσει ο driver μέσα στο cluster).
Εναλλακτικά, το αρχείο πρέπει να είναι ήδη μέσα στην εικόνα των εκτελεστών και να
δηλώνεται με πρόθεμα `local://`.

### Κατάσταση

Η τεχνική αλυσίδα μέχρι και την κλήση του `spark-submit` λειτουργεί. Απομένει η ορθή
διευθυνσιοδότηση του cluster και η προσβασιμότητα του παραγόμενου αρχείου από τον
executor.

---

## [011] 2026-08-01 — Χειροκίνητη επαλήθευση εκτέλεσης παραγόμενου προγράμματος

**Τύπος:** Έλεγχος λειτουργίας (καμία αλλαγή κώδικα)

### Σκοπός

Επειδή το Kubernetes cluster δεν είναι προσβάσιμο (βλ. παρακάτω), δοκιμάστηκε αν το
πρόγραμμα που παράγει το backend είναι **καθεαυτό εκτελέσιμο**, ανεξάρτητα από τον
μηχανισμό δρομολόγησης. Η εκτέλεση έγινε χειροκίνητα μέσα στο container του executor,
που διαθέτει ήδη Spark 3.5.0 και python3.

### Προσβασιμότητα του cluster

```
> docker exec -it executor kubectl cluster-info
Get "https://192.168.0.201:16443/api?timeout=32s": dial tcp 192.168.0.201:16443:
connect: connection refused

> docker exec -it executor curl -k -m 10 -s -o /dev/null -w "%{http_code}\n" \
      https://192.168.0.201:16443/version
000
```

Η διεύθυνση `192.168.0.201` ανήκει σε ιδιωτικό εύρος τοπικού δικτύου (RFC 1918) και
υπάρχει μόνο εντός του δικτύου της σχολής. Ο κωδικός `000` σημαίνει ότι δεν
πραγματοποιήθηκε καμία σύνδεση. Συμπέρασμα: **η εκτέλεση σε Kubernetes δεν μπορεί να
δοκιμαστεί εκτός του δικτύου του ιδρύματος**, ανεξάρτητα από διορθώσεις στον κώδικα.

### Διαδικασία δοκιμής

1. Δημιουργήθηκε δοκιμαστικό αρχείο `/tmp/customers.csv` (8 εγγραφές, στήλες `age`,
   `income`) μέσα στο container του executor.
2. Μέσω της φόρμας του UI δημιουργήθηκε pipeline `local-demo` με είσοδο
   `/tmp/customers.csv`, αλγόριθμο K-Means, χαρακτηριστικά `age, income`, `k = 2`.
3. Το παραγόμενο αρχείο αντιγράφηκε από το container του API στο container του executor
   (`docker cp`), επειδή ο τόμος `uploads_vol` δεν είναι προσαρτημένος στον executor.
4. Εκτελέστηκε: `spark-submit --master "local[*]" /tmp/gen.py`

### Αποτέλεσμα

Πρώτη εκτέλεση:

```
File "/tmp/gen.py", line 11, in <module>
    from pyspark.ml.feature import VectorAssembler
ModuleNotFoundError: No module named 'numpy'
```

Το `pyspark.ml` εξαρτάται από το `numpy`, το οποίο δεν περιλαμβάνεται στην εικόνα
`apache/spark:3.5.0`. Μετά από `pip install numpy` μέσα στο container, η εκτέλεση
ολοκληρώθηκε επιτυχώς.

### Συμπέρασμα

Ο παραγόμενος κώδικας είναι συντακτικά και σημασιολογικά ορθός και εκτελείται πραγματικά
από το Spark. Το σφάλμα εμφανίστηκε στη γραμμή 11, δηλαδή στα imports — η Python είχε
ήδη ξεκινήσει και είχε εντοπίσει το pyspark. Η αστοχία αφορούσε αποκλειστικά μια
απούσα εξάρτηση του περιβάλλοντος, όχι τον παραγόμενο κώδικα.

---

## [012] 2026-08-01 — Διορθώσεις που προέκυψαν από τις εγγραφές [010] και [011]

**Τύπος:** Τροποποίηση τριών υπαρχόντων αρχείων
**Αρχεία:** `executor/requirements2.txt`, `docker-compose.yml`, `backend/app.py`

Σημείωση: από τα τέσσερα ζητήματα που εντοπίστηκαν, υλοποιούνται τα **τρία** που
αποτελούν διορθώσεις σφαλμάτων. Η τέταρτη πρόταση (επιλογή τοπικής εκτέλεσης από τη
φόρμα) δεν υλοποιείται, κατόπιν απόφασης, καθώς αποτελεί νέα λειτουργία και όχι διόρθωση.

### Αλλαγή 12.1 — `executor/requirements2.txt`: προσθήκη `numpy`

**Πριν** (τελευταία γραμμή): `Werkzeug==3.0.1`
**Μετά:** προστέθηκε γραμμή `numpy==1.26.4`

Το `pyspark.ml`, δηλαδή ολόκληρη η βιβλιοθήκη μηχανικής μάθησης που χρησιμοποιούν τα
παραγόμενα προγράμματα, απαιτεί `numpy`. Χωρίς αυτό καμία εκτέλεση δεν είναι δυνατή
(εγγραφή [011]).

Επιλέχθηκε η έκδοση `1.26.4` και όχι η τελευταία διαθέσιμη: το PySpark 3.5.0 προηγείται
της έκδοσης 2.0 του numpy, η οποία εισήγαγε ασύμβατες αλλαγές στο API. Το καρφίτσωμα
συγκεκριμένης έκδοσης εξασφαλίζει επίσης αναπαραγωγιμότητα του build.

Το αρχείο διατηρήθηκε στην αρχική του κωδικοποίηση (UTF-16 LE με BOM, τερματισμοί
γραμμής CRLF) ώστε να μην αλλάξει τίποτε πέρα από το περιεχόμενο.

### Αλλαγή 12.2 — `docker-compose.yml`: προσάρτηση του `uploads_vol` στον executor

**Πριν:**

```yaml
  kubernetes-executor:
    image: kubernetes-executor:v0.1
    build: ./executor
    container_name: executor
    restart: always
    environment:
      ...
```

**Μετά:**

```yaml
  kubernetes-executor:
    image: kubernetes-executor:v0.1
    build: ./executor
    container_name: executor
    restart: always
    volumes:
      # Same mount point as in kubernetes-api, so the executable path stored
      # with a job resolves to the same file in both containers.
      - uploads_vol:/KubernetesAPI/backend/uploads
    environment:
      ...
```

Ο τόμος όπου αποθηκεύονται τα εκτελέσιμα ήταν προσαρτημένος αποκλειστικά στην υπηρεσία
`kubernetes-api`. Ο executor, που είναι ακριβώς η υπηρεσία που καλείται να τα υποβάλει,
δεν είχε καμία πρόσβαση σε αυτά. Πρόκειται για προϋπάρχον σφάλμα: αφορούσε εξίσου και τα
αρχεία `.jar` που ανέβαζε ο χρήστης στην αρχική ροή.

Το σημείο προσάρτησης είναι σκοπίμως **ταυτόσημο** με αυτό της υπηρεσίας
`kubernetes-api`, ώστε η διαδρομή που αποθηκεύεται στη βάση να παραπέμπει στο ίδιο αρχείο
και από τα δύο containers.

### Αλλαγή 12.3 — `backend/app.py`: απόλυτη διαδρομή στο `executable`

**Πριν:**

```python
    file_path = os.path.join(user_folder, filename)
```

**Μετά:**

```python
    # Absolute path: UPLOAD_FOLDER is relative ('./backend/uploads/') and would
    # only resolve correctly from the API container's working directory. The
    # executor runs from a different directory, so it needs the full path.
    file_path = os.path.abspath(os.path.join(user_folder, filename))
```

Η ρύθμιση `UPLOAD_FOLDER` είναι σχετική διαδρομή (`./backend/uploads/`). Στο container
του API ο τρέχων κατάλογος είναι `/KubernetesAPI` και η διαδρομή επιλύεται σωστά· στον
executor είναι `/KubernetesAPI/executor` και η επίλυση οδηγεί σε ανύπαρκτο σημείο.
Η `os.path.abspath()` αποθηκεύει πλήρη διαδρομή, η οποία σε συνδυασμό με την Αλλαγή 12.2
είναι έγκυρη και στα δύο containers.

### Έλεγχος

- `executor/requirements2.txt`: η κωδικοποίηση παραμένει UTF-16 LE με CRLF και το αρχείο
  αναγιγνώσκεται σωστά, με 18 εγγραφές αντί για 17.
- `docker-compose.yml`: αναλύεται ως έγκυρο YAML· ο τόμος εμφανίζεται και στις δύο
  υπηρεσίες με το ίδιο σημείο προσάρτησης.
- `backend/app.py`: περνά `ast.parse`. Η συμπεριφορά της `os.path.abspath` επαληθεύτηκε
  σε προσομοιωμένο κατάλογο: από τρέχοντα κατάλογο `X`, η σχετική διαδρομή
  `./backend/uploads/<user>/<file>.py` παράγει `X/backend/uploads/<user>/<file>.py`.

### Επίδραση στα υπάρχοντα jobs

Τα jobs που δημιουργήθηκαν πριν από την αλλαγή περιέχουν σχετικές διαδρομές και θα
συνεχίσουν να αποτυγχάνουν. Μόνο τα νέα jobs θα έχουν έγκυρη διαδρομή.

### Κατάσταση

Γραμμένο και ελεγμένο στατικά. Εκκρεμεί rebuild και επαλήθευση στο ζωντανό σύστημα.
Σημειώνεται ότι, με το cluster μη προσβάσιμο, η εκτέλεση θα εξακολουθεί να αποτυγχάνει
στο στάδιο σύνδεσης με το Kubernetes API — οι παρούσες διορθώσεις αίρουν τα εμπόδια που
προηγούνται αυτού του σταδίου.

---

## [013] 2026-08-02 — Σημείο επαναφοράς (checkpoint) στο git

**Τύπος:** Διαχείριση εκδόσεων (καμία αλλαγή κώδικα)

Πριν από την προσθήκη της επιλογής τοπικής εκτέλεσης, η κατάσταση του έργου
καταγράφηκε στο git ώστε να είναι δυνατή η επαναφορά.

- Commit: `802128e` — *Pipeline generation from JSON spec, option-schema fixes,
  executor rebuild*
- Ετικέτα: **`checkpoint-before-local-execution`**
- Επαναφορά με: `git checkout checkpoint-before-local-execution`

Παράλληλα επεκτάθηκε το `.gitignore` με `__pycache__/`, `*.pyc` και `/gen.py`
(προσωρινό αρχείο που δημιουργήθηκε κατά τη δοκιμή της εγγραφής [011]).

---

## [014] 2026-08-02 — Επιλογή τοπικής εκτέλεσης

**Τύπος:** Νέα λειτουργία (προσθετική)
**Αρχεία:** `executor/sample_customers.csv` (νέο), `executor/Dockerfile`,
`backend/app.py`, `frontend/.../pipeline-creation.component.ts` και `.html`

### Αιτία

Το Kubernetes cluster δεν είναι προσβάσιμο εκτός του δικτύου του ιδρύματος (εγγραφή
[011]), επομένως η λειτουργία του συστήματος δεν μπορεί να επιδειχθεί από άκρο σε άκρο.
Το `spark-submit` όμως δεν απαιτεί Kubernetes: με `--master local[*]` εκτελεί το
πρόγραμμα τοπικά, και το container του executor διαθέτει ήδη Spark 3.5.0 και python3.

Η προσθήκη είναι **αυστηρά προσθετική**: η προεπιλογή παραμένει η υποβολή στο cluster
και η υπάρχουσα συμπεριφορά δεν μεταβάλλεται. Η αφαίρεσή της συνίσταται στη διαγραφή
ενός πεδίου της φόρμας και ενός κλάδου `if` στο backend.

### Αλλαγή 14.1 — `executor/sample_customers.csv` (νέο αρχείο)

Δοκιμαστικό σύνολο δεδομένων 16 εγγραφών με στήλες `age`, `income`, `tenure`, `churned`.
Οι δύο πρώτες χρησιμεύουν ως χαρακτηριστικά για μη επιβλεπόμενους αλγορίθμους, η
τελευταία ως ετικέτα για επιβλεπόμενους. Έτσι μπορούν να επιδειχθούν και οι δύο
κατηγορίες χωρίς εξωτερική πηγή δεδομένων.

### Αλλαγή 14.2 — `executor/Dockerfile`

Προστέθηκε μετά την εγκατάσταση των εξαρτήσεων:

```dockerfile
# Sample dataset, so a pipeline can be run and demonstrated without needing
# any external storage. Referenced from the UI as /data/customers.csv
RUN mkdir -p /data && cp sample_customers.csv /data/customers.csv
```

Η αντιγραφή σε `/data/customers.csv` δίνει σύντομη και ευανάγνωστη διαδρομή για χρήση
στη φόρμα, αντί της `/KubernetesAPI/executor/sample_customers.csv` όπου το τοποθετεί η
εντολή `COPY`.

### Αλλαγή 14.3 — `backend/app.py`: παράμετρος `target`

**Πριν:**

```python
def build_job_options(pipeline_name, program_path, resources):
    resources = resources or {}
    return {
        'spark_path': resources.get('sparkPath', '/opt/spark/bin/spark-submit'),
        'master': resources.get('master', 'k8s://https://kubernetes.default.svc'),
        'deploy-mode': resources.get('deployMode', 'cluster'),
        ...
```

**Μετά:**

```python
def build_job_options(pipeline_name, program_path, resources):
    resources = resources or {}
    target = str(resources.get('target', 'kubernetes')).lower()

    if target == 'local':
        default_master = 'local[*]'
        default_deploy_mode = 'client'
    else:
        default_master = 'k8s://https://kubernetes.default.svc'
        default_deploy_mode = 'cluster'

    return {
        'spark_path': resources.get('sparkPath', '/opt/spark/bin/spark-submit'),
        'master': resources.get('master', default_master),
        'deploy-mode': resources.get('deployMode', default_deploy_mode),
        ...
```

Η απουσία της παραμέτρου `target` ισοδυναμεί με `'kubernetes'`, ώστε αιτήματα παλαιότερης
μορφής να συμπεριφέρονται ακριβώς όπως πριν.

### Αλλαγή 14.4 — Frontend

Στο `pipeline-creation.component.ts` προστέθηκαν ο πίνακας `targets` με τις δύο επιλογές,
το πεδίο `target` στη φόρμα με προεπιλογή `'kubernetes'`, η μεταβίβασή του στο
`buildResources()`, και ο βοηθός `isLocalTarget`.

Στο `pipeline-creation.component.html` προστέθηκε ενότητα *Execution* με το αντίστοιχο
πεδίο επιλογής, καθώς και υπόδειξη που εμφανίζεται μόνο σε τοπική εκτέλεση και
υποδεικνύει τη διαδρομή του δοκιμαστικού συνόλου δεδομένων.

### Έλεγχος

**Backend.** Με προσομοιωμένη βάση, τρία αιτήματα προς `POST /pipeline`:

| `target` | `master` που αποθηκεύτηκε | `deploy-mode` |
|----------|---------------------------|---------------|
| `kubernetes` | `k8s://https://kubernetes.default.svc` | `cluster` |
| `local` | `local[*]` | `client` |
| *(απουσιάζει)* | `k8s://https://kubernetes.default.svc` | `cluster` |

Επιβεβαιώθηκε επίσης ότι η αποθηκευμένη διαδρομή του εκτελέσιμου είναι απόλυτη
(Αλλαγή 12.3).

**Συμβατότητα με την επικύρωση.** Οι επιλογές τοπικής εκτέλεσης ελέγχθηκαν έναντι του
πραγματικού σχήματος μέσω του `SparkCommandGenerator`: καμία παραβίαση, και η παραγόμενη
εντολή είναι

```
/opt/spark/bin/spark-submit \
  --master local[*] \
  --deploy-mode client \
  --name local-demo \
  --conf spark.executor.instances=1 \
  ...
  /KubernetesAPI/backend/uploads/zikou@pve/abc-local-demo.py
```

Οι ρυθμίσεις `spark.kubernetes.*` παραμένουν στην εντολή αλλά αγνοούνται από το Spark
όταν ο master είναι τοπικός.

**Frontend.** `ngc -p tsconfig.app.json --noEmit` → 0 σφάλματα (28 δευτερόλεπτα), με
`strictTemplates` ενεργό.

### Κατάσταση

Γραμμένο και ελεγμένο στατικά. Εκκρεμεί rebuild και ζωντανή επίδειξη.

---

## [015] 2026-08-02 — Διόρθωση της έκδοσης `numpy` της Αλλαγής 12.1

**Τύπος:** Τροποποίηση υπάρχοντος αρχείου
**Αρχείο:** `executor/requirements2.txt`

### Αιτία

Το build του executor απέτυχε:

```
ERROR: No matching distribution found for numpy==1.26.4 (from -r requirements2.txt (line 18))
(from versions: 1.3.0, ..., 1.24.2, 1.24.3, 1.24.4)
```

Η λίστα των διαθέσιμων εκδόσεων που παραθέτει το pip τερματίζει στην `1.24.4`. Αυτό
αποκαλύπτει ότι ο διερμηνέας Python της εικόνας `apache/spark:3.5.0` είναι η έκδοση
**3.8**: το numpy 1.25 και νεότερα απαιτούν Python 3.9 ή νεότερη, οπότε το pip δεν τα
θεωρεί υποψήφια. Η επιλογή της `1.26.4` στην Αλλαγή 12.1 έγινε με την εσφαλμένη
παραδοχή ότι η εικόνα βασίζεται σε Ubuntu 22.04 με Python 3.10.

### Αλλαγή

**Πριν:** `numpy==1.26.4`
**Μετά:** `numpy==1.24.4`

Η `1.24.4` είναι η τελευταία έκδοση που υποστηρίζει Python 3.8 και ταυτόχρονα προηγείται
της σειράς 2.x, διατηρώντας έτσι τη συμβατότητα με το PySpark 3.5.0 που ήταν ο αρχικός
λόγος καρφιτσώματος συγκεκριμένης έκδοσης.

### Παρατήρηση

Το σφάλμα αυτό δείχνει τη χρησιμότητα του καρφιτσώματος εκδόσεων: η αστοχία εμφανίστηκε
κατά το build, με σαφές μήνυμα, αντί να εγκατασταθεί σιωπηλά μια τυχαία έκδοση και να
προκύψει δυσδιάγνωστη αποτυχία κατά την εκτέλεση.

### Κατάσταση

Διορθωμένο. Εκκρεμεί επανάληψη του build.

---

## [016] 2026-08-02 — Μονάδα μέτρησης στις ρυθμίσεις μνήμης

**Τύπος:** Τροποποίηση υπάρχοντος αρχείου
**Αρχείο:** `executor/SparkCommandGenerator.py`

### Αιτία

Με το build διορθωμένο και την τοπική εκτέλεση ενεργή, το `spark-submit` εκτελέστηκε και
το Spark έφτασε μέχρι τη δημιουργία του `SparkContext`, όπου απέτυχε:

```
org.apache.spark.SparkIllegalArgumentException: [INVALID_EXECUTOR_MEMORY]
Executor memory 512 must be at least 471859200.
```

Το σχήμα επιλογών (`database/Kubernetes.Options.json`) ορίζει την
`spark.executor.memory` ως ακέραιο με εύρος `512` έως `10240`, δηλαδή σε **megabytes**.
Η μονάδα όμως υπονοείται και δεν καταγράφεται πουθενά στην τιμή. Το Spark, όταν λαμβάνει
αριθμό χωρίς επίθεμα μονάδας, τον ερμηνεύει ως **bytes**. Έτσι το `512` γινόταν 512 bytes
αντί για 512 MiB, τιμή κατά πολύ μικρότερη του ελάχιστου επιτρεπτού.

Πρόκειται για προϋπάρχον σφάλμα: αφορά κάθε job, ανεξάρτητα από τον τρόπο εκτέλεσης, και
θα εμφανιζόταν εξίσου κατά την υποβολή σε cluster.

### Αλλαγή

Προστέθηκε σταθερά με τα κλειδιά που εκφράζουν μέγεθος μνήμης:

```python
_MEMORY_KEYS = {'spark.executor.memory', 'spark.driver.memory'}
```

και βοηθητική συνάρτηση που προσθέτει τη μονάδα:

```python
def _conf_value(key, value):
    """ Renders a conf value, appending the megabyte unit to bare memory numbers. """
    if key in _MEMORY_KEYS and str(value).strip().isdigit():
        return f'{int(value)}m'
    return value
```

**Πριν:**

```python
    for conf_key, conf_value in (options.get('conf') or {}).items():
        args += ['--conf', f'{conf_key}={conf_value}']
```

**Μετά:**

```python
    for conf_key, conf_value in (options.get('conf') or {}).items():
        args += ['--conf', f'{conf_key}={_conf_value(conf_key, conf_value)}']
```

### Σχεδιαστική επιλογή

Η μετατροπή τοποθετήθηκε στον generator και όχι στη βάση, ώστε το σχήμα να παραμείνει ως
έχει (ακέραιος με ελέγξιμο αριθμητικό εύρος — κάτι που θα χανόταν αν η τιμή γινόταν
συμβολοσειρά τύπου `'512m'`). Ο έλεγχος `isdigit()` αφήνει ανέπαφες τιμές που φέρουν ήδη
μονάδα, ώστε μια μελλοντική τιμή `'2g'` να περάσει αυτούσια.

### Έλεγχος

| Είσοδος | Παραγόμενο όρισμα |
|---------|-------------------|
| `spark.executor.memory: 512` | `--conf spark.executor.memory=512m` |
| `spark.executor.memory: '2g'` | `--conf spark.executor.memory=2g` |

Η επικύρωση έναντι του πραγματικού σχήματος εξακολουθεί να περνά χωρίς σφάλματα.

### Συναφές εύρημα (δεν διορθώθηκε)

Το job της αποτυχημένης εκτέλεσης καταγράφηκε στη βάση με `status: 'finished'` και
`priority: 0`, παρότι το πρόγραμμα τερμάτισε με σφάλμα. Η αιτία βρίσκεται στο
`executor/JobExecutor.py`:

```python
result = subprocess.run(spark_submit_args, shell=False, capture_output=True, text=True)
print(result.stderr)
if result.stdout == '':
    result.stdout = "Subprocess didn't run"
```

Η επιτυχία κρίνεται από το αν η τυπική έξοδος είναι κενή, αντί από τον κωδικό εξόδου της
διεργασίας (`result.returncode`). Επειδή η ιχνηλάτηση σφάλματος της Python γράφτηκε στην
τυπική έξοδο, η έξοδος δεν ήταν κενή και το job θεωρήθηκε επιτυχημένο. Καταγράφεται ως
εκκρεμότητα προς απόφαση.

---

## [017] 2026-08-02 — Επιτυχής εκτέλεση από άκρο σε άκρο

**Τύπος:** Έλεγχος λειτουργίας (καμία αλλαγή κώδικα)

### Σενάριο

Μέσω της φόρμας του UI, χωρίς καμία χειροκίνητη παρέμβαση:

| Πεδίο | Τιμή |
|-------|------|
| Pipeline Name | `customer-segments` |
| Data Path | `/data/customers.csv` |
| Format | `csv`, με header και συμπερασμό τύπων |
| Algorithm | K-Means |
| Feature Columns | `age, income` |
| k | 2 |
| Execution target | Local |

### Αποτέλεσμα

```
+---+------+------+-------+---------------+----------+
|age|income|tenure|churned|features       |prediction|
+---+------+------+-------+---------------+----------+
|25 |30000 |1     |1      |[25.0,30000.0] |1         |
|34 |52000 |4     |0      |[34.0,52000.0] |1         |
|45 |61000 |9     |0      |[45.0,61000.0] |1         |
|23 |28000 |1     |1      |[23.0,28000.0] |1         |
|52 |90000 |15    |0      |[52.0,90000.0] |0         |
|38 |58000 |6     |0      |[38.0,58000.0] |1         |
|29 |41000 |2     |1      |[29.0,41000.0] |1         |
|41 |72000 |11    |0      |[41.0,72000.0] |0         |
|27 |33000 |1     |1      |[27.0,33000.0] |1         |
|48 |84000 |13    |0      |[48.0,84000.0] |0         |
|31 |45000 |3     |1      |[31.0,45000.0] |1         |
|57 |95000 |20    |0      |[57.0,95000.0] |0         |
|22 |26000 |1     |1      |[22.0,26000.0] |1         |
|44 |69000 |10    |0      |[44.0,69000.0] |0         |
|36 |55000 |5     |0      |[36.0,55000.0] |1         |
|60 |102000|25    |0      |[60.0,102000.0]|0         |
+---+------+------+-------+---------------+----------+
```

### Σχολιασμός

Η στήλη `features` είναι το διάνυσμα χαρακτηριστικών που συνέθεσε ο `VectorAssembler`, και
η `prediction` η ομάδα που ανέθεσε ο K-Means. Ο διαχωρισμός έγινε κυρίως βάσει
εισοδήματος: η ομάδα 0 περιλαμβάνει τους πελάτες με εισόδημα 69.000 και άνω, η ομάδα 1
τους υπόλοιπους. Αξιοσημείωτο είναι ότι όλοι οι πελάτες της ομάδας 0 έχουν `churned = 0`,
δηλαδή ο μη επιβλεπόμενος αλγόριθμος εντόπισε ομαδοποίηση που συσχετίζεται με την
αποχώρηση, χωρίς να έχει πρόσβαση στη σχετική στήλη.

### Επαληθευμένη αλυσίδα

1. Ο χρήστης συμπληρώνει φόρμα στο Angular frontend.
2. Το frontend κατασκευάζει την προδιαγραφή JSON.
3. Το backend την επικυρώνει και παράγει πρόγραμμα PySpark.
4. Το πρόγραμμα αποθηκεύεται σε κοινόχρηστο τόμο και καταχωρείται job στη MongoDB.
5. Ο executor εντοπίζει το job, επικυρώνει τις επιλογές του έναντι του σχήματος στη βάση,
   κατασκευάζει την εντολή `spark-submit` ως λίστα ορισμάτων και την εκτελεί.
6. Το Spark εκτελεί το πρόγραμμα και τα αποτελέσματα καταγράφονται στη MongoDB.

Το μόνο στάδιο που δεν έχει επαληθευτεί είναι η δρομολόγηση σε Kubernetes cluster, η
οποία διαφέρει αποκλειστικά ως προς την τιμή της επιλογής `master` και απαιτεί πρόσβαση
στο δίκτυο του ιδρύματος (εγγραφή [011]).

---

## [018] 2026-08-03 — Πολλαπλά στάδια, μετασχηματισμοί χαρακτηριστικών, ομαδοποίηση αλγορίθμων

**Τύπος:** Νέα λειτουργία
**Αρχεία:** `backend/SparkProgramGenerator.py`,
`frontend/.../pipeline-creation.component.{ts,html,css}`

### Αιτία

Τρεις νέες απαιτήσεις από τον επιβλέποντα:

1. Δεύτερη λίστα αλγορίθμων, ώστε ο δεύτερος να λαμβάνει ως είσοδο την έξοδο του πρώτου.
2. Min-max scaling και άλλοι μετασχηματισμοί.
3. Διαχωρισμός των αλγορίθμων ανά κατηγορία (clustering, classification, regression).

### Απαίτηση 1 — Αλυσίδωση σταδίων

**Αλλαγή στην προδιαγραφή.** Το πεδίο `algorithm` (ένα αντικείμενο) αντικαταστάθηκε από
`stages` (λίστα αντικειμένων). Η συνάρτηση `_stages_of()` δέχεται και τις δύο μορφές, ώστε
προδιαγραφές παλαιότερου τύπου να εξακολουθούν να λειτουργούν ως pipeline ενός σταδίου.

**Απομόνωση στηλών.** Κάθε στάδιο μετασχηματίζει το DataFrame και προσθέτει δικές του
στήλες. Για να μη συγκρούονται, το στάδιο N γράφει σε `features_N` και `prediction_N`, και
για τους ταξινομητές επιπλέον σε `raw_prediction_N` και `probability_N`. Χωρίς αυτό, δύο
ταξινομητές θα προσπαθούσαν και οι δύο να γράψουν σε στήλη `prediction`.

**Πώς επιτυγχάνεται η αλυσίδωση.** Επειδή κάθε στάδιο ενεργεί πάνω στο ίδιο DataFrame
(`data`), το στάδιο 2 βλέπει όλες τις στήλες που παρήγαγε το στάδιο 1 και μπορεί να
δηλώσει το `prediction_1` ανάμεσα στα δικά του χαρακτηριστικά.

Παράδειγμα παραγόμενου κώδικα:

```python
# --- stage 1: kmeans ---
assembler_1 = VectorAssembler(inputCols=['age', 'income'], outputCol='features_1_raw')
data = assembler_1.transform(data)
scaler_1 = MinMaxScaler(inputCol='features_1_raw', outputCol='features_1')
data = scaler_1.fit(data).transform(data)
estimator_1 = KMeans(featuresCol='features_1', predictionCol='prediction_1', k=2, maxIter=20, seed=1)
model_1 = estimator_1.fit(data)
data = model_1.transform(data)

# --- stage 2: random_forest ---
assembler_2 = VectorAssembler(inputCols=['age', 'income', 'prediction_1'], outputCol='features_2')
data = assembler_2.transform(data)
estimator_2 = RandomForestClassifier(featuresCol='features_2', labelCol='churned',
                                     predictionCol='prediction_2', rawPredictionCol='raw_prediction_2',
                                     probabilityCol='probability_2', numTrees=20, maxDepth=5, seed=1)
model_2 = estimator_2.fit(data)
data = model_2.transform(data)
```

**Στο frontend** τα στάδια υλοποιούνται ως `FormArray`, με κουμπί προσθήκης και αφαίρεσης
ανά στάδιο. Το πρώτο στάδιο δεν αφαιρείται. Κάθε στάδιο εμφανίζει υπόδειξη με τις στήλες
που έχουν παραγάγει τα προηγούμενα.

### Απαίτηση 2 — Μετασχηματισμοί χαρακτηριστικών

Προστέθηκαν σε δύο επίπεδα, ανάλογα με το πού ανήκουν εννοιολογικά.

**Κλιμάκωση, ανά στάδιο.** Νέος κατάλογος `SCALERS` με `MinMaxScaler` και
`StandardScaler`. Επιλέγεται από πεδίο *Feature scaling* μέσα στο κάθε στάδιο και
εφαρμόζεται στο συγκεντρωμένο διάνυσμα χαρακτηριστικών, ανάμεσα στον `VectorAssembler` και
τον αλγόριθμο. Όταν υπάρχει κλιμάκωση, ο assembler γράφει σε `features_N_raw` και ο
scaler παράγει το `features_N`.

**Κωδικοποίηση, ως βήματα.** Νέος κατάλογος `COLUMN_TRANSFORMERS` με `StringIndexer` και
`OneHotEncoder`. Προστέθηκαν ως νέοι τύποι στη λίστα μετασχηματισμών, με πεδία
`inputCol` και `outputCol`. Ενεργούν σε επίπεδο στήλης, πριν συγκεντρωθεί οποιοδήποτε
διάνυσμα — γι' αυτό ανήκουν εκεί και όχι στο στάδιο.

Και οι δύο κατηγορίες είναι Estimators του Spark, δηλαδή μαθαίνουν πρώτα από τα δεδομένα:
ο παραγόμενος κώδικας καλεί `.fit(...).transform(...)` και όχι σκέτο `.transform(...)`.

### Απαίτηση 3 — Κατηγοριοποίηση

Στο πεδίο κάθε ορισμού αλγορίθμου προστέθηκε `category`. Ο πίνακας `CATEGORIES` ορίζει τη
σειρά και τα εμφανιζόμενα ονόματα. Στη φόρμα η λίστα χρησιμοποιεί `mat-optgroup`, οπότε οι
αλγόριθμοι εμφανίζονται ομαδοποιημένοι με επικεφαλίδες.

### Επέκταση του καταλόγου αλγορίθμων

Με την ευκαιρία της κατηγοριοποίησης, ο κατάλογος επεκτάθηκε από τέσσερις σε οκτώ:

| Κατηγορία | Αλγόριθμοι |
|-----------|------------|
| Clustering | K-Means, Bisecting K-Means, Gaussian Mixture |
| Classification | Logistic Regression, Decision Tree, Random Forest |
| Regression | Linear Regression, Decision Tree Regressor |

Η επέκταση απαίτησε μόνο νέες εγγραφές στα δύο dictionary (backend και frontend), χωρίς
καμία αλλαγή στη λογική παραγωγής — επιβεβαιώνοντας τη σχεδιαστική επιλογή της εγγραφής
[002].

### Νέο πεδίο στους ορισμούς: `prediction_cols`

Κάθε αλγόριθμος δηλώνει ποιες παράμετροι εξόδου του χρειάζονται μοναδικό όνομα ανά
στάδιο. Οι ταξινομητές δηλώνουν τρεις (`predictionCol`, `rawPredictionCol`,
`probabilityCol`), οι υπόλοιποι μία. Χωρίς αυτό, δύο ταξινομητές στο ίδιο pipeline θα
συγκρούονταν στις στήλες `rawPrediction` και `probability`.

### Δυναμικές εισαγωγές

Η συνάρτηση `_imports()` συγκεντρώνει τις απαιτούμενες γραμμές `import` από όλα τα
στάδια και βήματα, χωρίς επαναλήψεις. Το παραγόμενο πρόγραμμα εισάγει μόνο ό,τι
πραγματικά χρησιμοποιεί.

### Έλεγχος

**Generator.** Δοκιμάστηκε με pipeline δύο σταδίων (K-Means με min-max scaling, και στη
συνέχεια Random Forest που χρησιμοποιεί το `prediction_1` ως χαρακτηριστικό), με
pipeline που περιλαμβάνει `StringIndexer` και `OneHotEncoder`, και με προδιαγραφή
παλαιού τύπου (`algorithm` αντί για `stages`). Σε όλες τις περιπτώσεις ο παραγόμενος
κώδικας μεταγλωττίζεται.

Έξι άκυρες περιπτώσεις εντοπίστηκαν σωστά:

| Σενάριο | Μήνυμα |
|---------|--------|
| Καθόλου στάδια | `stages: at least one algorithm stage is required` |
| Άγνωστος αλγόριθμος | `stages[0].name: 'svm' is not one of [...]` |
| Άγνωστος scaler | `stages[0].scaler: 'robust' is not one of ['minmax', 'standard']` |
| Απουσία ετικέτας | `stages[0].label: is required for random_forest` |
| `StringIndexer` χωρίς `outputCol` | `steps[0].outputCol: is required for string_indexer` |
| Παράμετρος άλλου αλγορίθμου | `stages[0].params.numTrees: not supported by kmeans` |

**Endpoints.** Το `/preview_program` επιστρέφει σωστά τον διψήφιο κώδικα, το `/pipeline`
γράφει το αρχείο και καταχωρεί job με `master = local[*]`, και άκυρη προδιαγραφή
απορρίπτεται με `400`.

**Frontend.** `ngc -p tsconfig.app.json --noEmit` → 0 σφάλματα (26 δευτερόλεπτα), με
`strictTemplates` ενεργό.

### Κατάσταση

Ολοκληρωμένο και ελεγμένο στατικά. Εκκρεμεί rebuild και δοκιμή στον browser.

---

## [019] 2026-08-03 — Επαλήθευση pipeline δύο σταδίων

**Τύπος:** Έλεγχος λειτουργίας (καμία αλλαγή κώδικα)

### Σενάριο

Μέσω της φόρμας: είσοδος `/data/customers.csv`, ένα βήμα `dropna`, και δύο στάδια —
K-Means με χαρακτηριστικά `age, income`, κλιμάκωση Min-Max και `k = 2`, ακολουθούμενο από
Random Forest με χαρακτηριστικά `age, income, prediction_1` και ετικέτα `churned`.
Εκτέλεση τοπικά.

### Αποτέλεσμα (απόσπασμα)

```
+---+------+-------+---------------+------------------------------------------+------------+-------------------+------------+
|age|income|churned|features_1_raw |features_1                                |prediction_1|features_2         |prediction_2|
+---+------+-------+---------------+------------------------------------------+------------+-------------------+------------+
|25 |30000 |1      |[25.0,30000.0] |[0.07894736842105263,0.052631578947368425]|1           |[25.0,30000.0,1.0] |1.0         |
|45 |61000 |0      |[45.0,61000.0] |[0.6052631578947368,0.46052631578947373]  |0           |[45.0,61000.0,0.0] |0.0         |
|22 |26000 |1      |[22.0,26000.0] |(2,[],[])                                 |1           |[22.0,26000.0,1.0] |1.0         |
|60 |102000|0      |[60.0,102000.0]|[1.0,1.0]                                 |0           |[60.0,102000.0,0.0]|0.0         |
+---+------+-------+---------------+------------------------------------------+------------+-------------------+------------+
```

### Επαληθευμένα σημεία

**Η αλυσίδωση λειτουργεί.** Η στήλη `features_2` περιέχει τρία στοιχεία, εκ των οποίων το
τρίτο είναι η τιμή του `prediction_1`. Το δεύτερο στάδιο κατανάλωσε την έξοδο του πρώτου.

**Η κλιμάκωση λειτουργεί.** Η `features_1_raw` διατηρεί τις αρχικές τιμές και η
`features_1` τις ίδιες απεικονισμένες στο διάστημα [0, 1].

**Η απομόνωση στηλών λειτουργεί.** Παρήχθησαν χωριστά `prediction_1`, `prediction_2`,
`raw_prediction_2` και `probability_2`, χωρίς σύγκρουση ονομάτων.

### Δύο παρατηρήσεις

**Η τιμή `(2,[],[])`.** Δεν πρόκειται για σφάλμα, αλλά για την αραιή αναπαράσταση του
διανύσματος `[0.0, 0.0]`: «διάνυσμα μήκους 2, καμία μη μηδενική θέση». Η συγκεκριμένη
εγγραφή έχει την ελάχιστη τιμή και στα δύο χαρακτηριστικά, οπότε ο `MinMaxScaler` την
απεικονίζει στο μηδέν και το Spark την αποθηκεύει αραιά.

**Η φαινομενικά τέλεια ακρίβεια.** Η `prediction_2` ταυτίζεται με τη `churned` σε όλες τις
εγγραφές. Αυτό **δεν** αποτελεί ένδειξη ποιότητας του μοντέλου: η εκπαίδευση και η
αξιολόγηση έγιναν στο ίδιο σύνολο των 16 εγγραφών, χωρίς διαχωρισμό σε σύνολα εκπαίδευσης
και ελέγχου, ενώ επιπλέον το `prediction_1` είναι από κατασκευής ισχυρός προγνωστικός
δείκτης. Ο διαχωρισμός train/test και οι μετρικές αξιολόγησης παραμένουν εκτός
υλοποίησης — το σύστημα αποδεικνύει την **ορθή κατασκευή και εκτέλεση** του pipeline, όχι
την εγκυρότητα του παραγόμενου μοντέλου.

---

## [020] 2026-10-20 — `docker-compose.yml` — Πρόσβαση του executor στο τοπικό Kubernetes

**Τύπος:** Τροποποίηση υπάρχοντος αρχείου (καταγράφεται εκ των υστέρων· η αλλαγή έγινε κατά
τις δοκιμές με το `pi.py`, πριν από τις εγγραφές [021]–[026])
**Αρχείο:** `docker-compose.yml`, υπηρεσία `kubernetes-executor`

### Αιτία

Η απόφαση ήταν να τρέχουν οι εργασίες τοπικά, στο Kubernetes του Docker Desktop. Ο executor
όμως δεν μπορούσε να επικοινωνήσει με τον API server του cluster: το όνομα
`kubernetes.docker.internal` δεν επιλυόταν μέσα στο container, και το kubeconfig που
δημιουργεί το `kubectl_config.sh` κατά το build δείχνει στο cluster του εργαστηρίου.

### Μετά

```yaml
    extra_hosts:
      - "kubernetes.docker.internal:host-gateway"
    volumes:
      - uploads_vol:/KubernetesAPI/backend/uploads
      - C:\Users\andre\.kube\config:/root/.kube/config:ro
```

### Αιτιολόγηση

| # | Αλλαγή | Σκοπός |
|---|--------|--------|
| 1 | `extra_hosts` | Το `kubernetes.docker.internal` αντιστοιχίζεται στον υπολογιστή που φιλοξενεί το Docker, όπου ακούει ο API server (θύρα 6443). |
| 2 | Mount του `~/.kube/config` | Ο executor (`spark-submit` και `kubectl`) χρησιμοποιεί το kubeconfig που δημιούργησε το Docker Desktop. Το mount είναι `:ro` και επισκιάζει, χωρίς να το διαγράφει, το αρχείο που γράφεται κατά το build. |

Η υποβολή επαληθεύτηκε με το `pi.py`: το driver pod και το executor pod ολοκληρώθηκαν.

---

## [021] 2026-10-20 — Κοινός φάκελος (hostPath) για τα pipelines

**Τύπος:** Τροποποίηση υπαρχόντων αρχείων
**Αρχεία:** `docker-compose.yml`, `backend/app.py`, `executor/JobExecutor.py`, `executor/Dockerfile`,
νέο `executor/sample_reviews.csv`

### Αιτία

Στη λειτουργία cluster ο driver και οι executors τρέχουν ως pods, που δεν βλέπουν τα
αρχεία των containers. Το παραγόμενο πρόγραμμα ήταν αποθηκευμένο στο named volume
`uploads_vol` και τα δεδομένα δείγματος στο `/data` της εικόνας του executor, οπότε ένα
pod δεν μπορούσε να βρει ούτε το ένα ούτε το άλλο. Ως τώρα μπορούσε να τρέξει σε pods μόνο
το `pi.py`, που υπάρχει ήδη μέσα στην εικόνα του Spark.

### Λύση

Ένας φάκελος των Windows, `C:\Users\andre\spark-shared`, φαίνεται **στην ίδια διαδρομή**
(`/mnt/shared`) από όλους:

| Ποιος | Πώς |
|-------|-----|
| container `api` | bind mount στο `docker-compose.yml` |
| container `executor` | bind mount στο `docker-compose.yml` |
| driver pod, executor pods | volume `hostPath`, μέσω των ρυθμίσεων `spark.kubernetes.{driver,executor}.volumes.hostPath.shared.*` |

Το Docker Desktop εκθέτει τον δίσκο `C:` στον κόμβο του Kubernetes ως
`/run/desktop/mnt/host/c`, γι' αυτό η διαδρομή του hostPath είναι
`/run/desktop/mnt/host/c/Users/andre/spark-shared`. Επειδή η διαδρομή είναι ίδια παντού,
καμία διαδρομή αρχείου δεν χρειάζεται μετάφραση.

Οργάνωση του φακέλου:

```
spark-shared/
  programs/<user>/<id>-<όνομα>.py   παραγόμενα προγράμματα
  data/customers.csv, reviews.csv   δεδομένα δείγματος (αντιγράφονται από τον executor)
  results/<id>-<όνομα>.json         προεπισκόπηση αποτελέσματος (βλ. [025])
```

### Αλλαγές ανά αρχείο

| Αρχείο | Αλλαγή |
|--------|--------|
| `docker-compose.yml` | Bind mount του φακέλου στο `api` και στον `executor`. Νέες μεταβλητές περιβάλλοντος στο `api`: `SHARED_DIR`, `SHARED_NODE_DIR`, `K8S_MASTER`, `K8S_NODE_HOSTNAME`, `SPARK_IMAGE`. |
| `backend/app.py` | Τα προγράμματα γράφονται στο `programs/` του κοινού φακέλου (νέες βοηθητικές `pipeline_paths`, `results_path_for`). Στο `build_job_options`, για στόχο Kubernetes: master `k8s://https://kubernetes.docker.internal:6443` αντί για `kubernetes.default.svc` (που επιλύεται μόνο μέσα στο cluster), node selector `docker-desktop` αντί για `k8s-1` (κόμβος του cluster του εργαστηρίου), executable `local:///mnt/shared/...`, ρυθμίσεις hostPath για driver και executors, σταθερό όνομα driver pod (βλ. [024]) και `pullPolicy=IfNotPresent`. Οι τιμές διαβάζονται από το περιβάλλον, ώστε η μετάβαση στο cluster του εργαστηρίου να γίνεται χωρίς αλλαγή κώδικα. |
| `executor/JobExecutor.py` | Νέα `prepare_shared_folder()`: κατά την εκκίνηση δημιουργεί τους υποφακέλους και αντιγράφει τα δεδομένα δείγματος, αν δεν υπάρχουν ήδη. |
| `executor/Dockerfile`, `sample_reviews.csv` | Δεύτερο σύνολο δεδομένων δείγματος, με κείμενο (`id, text, positive`), για τους αλγορίθμους TF-IDF και Word2Vec ([023]). Πρόκειται για 16 σύντομες, επινοημένες φράσεις, που χρησιμεύουν μόνο για επίδειξη. |

Η λειτουργία «Submit existing application» (ανέβασμα έτοιμου εκτελέσιμου) δεν αλλάζει
και συνεχίζει να χρησιμοποιεί το `uploads_vol`.

---

## [022] 2026-10-20 — `spark-image/Dockerfile` — Εικόνα για τα pods

**Τύπος:** Νέο αρχείο, και νέα υπηρεσία `spark-ml` στο `docker-compose.yml` (μόνο για build)

### Αιτία

Η εικόνα `apache/spark:3.5.0` περιέχει Python αλλά όχι `numpy`. Το `pyspark.ml` εισάγει
το `numpy` ήδη από το `import`, οπότε κάθε πρόγραμμα με MLlib θα αποτύγχανε στο driver pod
με `ModuleNotFoundError: No module named 'numpy'`. Το `pi.py` δεν χρησιμοποιεί MLlib και
γι' αυτό δεν είχε εμφανίσει το πρόβλημα.

### Λύση

Μια μικρή εικόνα `spark-ml:3.5.0`: `apache/spark:3.5.0` + `numpy==1.24.4`, η ίδια έκδοση
με τον executor ([015]). Το Kubernetes του Docker Desktop (provisioner kubeadm)
χρησιμοποιεί τις τοπικές εικόνες του Docker, οπότε δεν χρειάζεται registry. Η εικόνα
χτίζεται μία φορά:

```
docker compose --profile images build spark-ml
```

Η υπηρεσία έχει `profiles: ["images"]`, άρα ένα απλό `docker compose up` δεν τη χτίζει
και δεν την ξεκινά. Η προεπιλεγμένη εικόνα της φόρμας άλλαξε από `apache/spark:3.5.0`
σε `spark-ml:3.5.0`.

---

## [023] 2026-10-20 — Κατηγορία → αλγόριθμος, και κατηγορία Feature Extraction

**Τύπος:** Τροποποίηση υπαρχόντων αρχείων
**Αρχεία:** `backend/SparkProgramGenerator.py`, `frontend/.../pipeline-creation.component.{ts,html,css}`

### Αιτία

Οι αλγόριθμοι εμφανίζονταν σε μία λίστα, χωρισμένη σε ομάδες ([018]). Ζητήθηκαν δύο
ξεχωριστές λίστες (πρώτα κατηγορία, μετά μόνο οι αλγόριθμοι αυτής) και μια τέταρτη
κατηγορία, η εξαγωγή χαρακτηριστικών.

### Φόρμα

Κάθε στάδιο έχει πλέον δύο πεδία: **Category** (Clustering, Classification, Regression,
Feature Extraction) και **Algorithm**, που δείχνει μόνο τους αλγορίθμους της κατηγορίας.
Όταν αλλάζει η κατηγορία, επιλέγεται αυτόματα ο πρώτος αλγόριθμός της. Κάτω από κάθε
στάδιο εμφανίζεται η στήλη που προσθέτει (π.χ. `pca_1`), και η υπόδειξη αλυσίδωσης
δείχνει τη σωστή στήλη κάθε προηγούμενου σταδίου (παλιότερα έδειχνε πάντα
`prediction_N`). Για τους αλγορίθμους κειμένου, το πεδίο γίνεται «Text Column» και η
κανονικοποίηση κρύβεται.

Μια διόρθωση στο ίδιο σημείο: οι συναρτήσεις που ξαναχτίζουν τις παραμέτρους ενός σταδίου
δέχονταν τη **θέση** του σταδίου, όπως ήταν τη στιγμή της δημιουργίας του. Αν διαγραφόταν
ένα προηγούμενο στάδιο, η αλλαγή αλγορίθμου ενημέρωνε λάθος στάδιο. Τώρα δέχονται το ίδιο
το στάδιο (`FormGroup`).

### Νέοι αλγόριθμοι

| Τιμή | Κλάση(εις) MLlib | Είσοδος | Προσθέτει |
|------|------------------|---------|-----------|
| `pca` | `PCA` | αριθμητικές στήλες (assembler, προαιρετικά κανονικοποίηση) | `pca_N` |
| `chisq_selector` | `ChiSqSelector` (απαιτεί label) | αριθμητικές στήλες | `selected_N` |
| `tf_idf` | `Tokenizer` → `HashingTF` → `IDF` | μία στήλη κειμένου | `tfidf_N` |
| `word2vec` | `Tokenizer` → `Word2Vec` | μία στήλη κειμένου | `w2v_N` |

Οι στήλες αυτές είναι διανύσματα, οπότε ένα επόμενο στάδιο μπορεί να τις βάλει στα
χαρακτηριστικά του (π.χ. K-Means πάνω στο `pca_1`).

### Generator

- Ο κατάλογος `ALGORITHMS` απέκτησε τα πεδία `input` (`vector`/`text`), `input_param` και
  `output_col` για τους νέους αλγορίθμους.
- Η `_stage_lines` χωρίστηκε σε `_vector_input_lines` (assembler + κανονικοποίηση, όπως
  πριν) και `_text_stage_lines` (tokenizer κ.λπ.).
- Νέοι έλεγχοι: οι αλγόριθμοι κειμένου δέχονται **ακριβώς μία** στήλη και καμία
  κανονικοποίηση.
- Κατά την εγγραφή του αποτελέσματος, οι βοηθητικές στήλες (λέξεις, συχνότητες, διανύσματα
  εισόδου) αφαιρούνται, όπως και πριν. Οι εξαγόμενες στήλες **κρατιούνται** και
  μετατρέπονται σε πίνακες αριθμών (σε CSV, σε κείμενο `"[0.1, 0.2]"`). Η μετατροπή γίνεται
  με ένα μικρό UDF και όχι με το `pyspark.ml.functions.vector_to_array`: το import εκείνου
  του module απαιτεί `pandas`, που δεν υπάρχει στις εικόνες. Αυτό διαπιστώθηκε κατά τη
  δοκιμή ([026]).

---

## [024] 2026-10-20 — `executor/JobExecutor.py` — Επιτυχία, αποτυχία και log ενός job

**Τύπος:** Τροποποίηση υπάρχοντος αρχείου

### Αιτία

1. Ο executor θεωρούσε ότι ένα job πέτυχε όταν το `spark-submit` είχε γράψει κάτι στο
   stdout. Στη λειτουργία cluster το `spark-submit` γράφει μόνο στο stderr, οπότε ένα job
   που ολοκληρωνόταν κανονικά θα σημειωνόταν ως αποτυχημένο.
2. Αποθηκευόταν μόνο το stdout του `spark-submit`. Στη λειτουργία cluster, η έξοδος του
   προγράμματος βρίσκεται στο log του driver pod, γι' αυτό και ως τώρα τη διαβάζαμε από το
   cmd με `kubectl logs`.

### Λύση

- Για τα jobs Kubernetes που έχουν σταθερό όνομα driver pod
  (`spark.kubernetes.driver.pod.name`, [021]):
  - πριν από την υποβολή διαγράφεται τυχόν pod με το ίδιο όνομα από προηγούμενη απόπειρα·
  - μετά την υποβολή, η επιτυχία κρίνεται από τη φάση του pod (`Succeeded`)·
  - ως log αποθηκεύεται το `kubectl logs` του driver pod, και σε αποτυχία προστίθεται και η
    έξοδος του `spark-submit`.
- Για τις τοπικές εκτελέσεις, η επιτυχία κρίνεται από τον κωδικό εξόδου του `spark-submit`,
  και σε αποτυχία το log συμπληρώνεται με το stderr.
- Τα logs περικόπτονται στους τελευταίους 200.000 χαρακτήρες, λόγω του ορίου 16 MB ανά
  έγγραφο της MongoDB.
- Η `job_executor` επιστρέφει πλέον `True`/`False` αντί για το κείμενο
  `"Subprocess didn't run"`.

Τα jobs της λειτουργίας «Submit existing application» δεν έχουν σταθερό όνομα pod και
κρίνονται από τον κωδικό εξόδου.

---

## [025] 2026-10-20 — Κουμπί αποτελεσμάτων στα ολοκληρωμένα jobs

**Τύπος:** Τροποποίηση υπαρχόντων αρχείων και νέο component
**Αρχεία:** `backend/SparkProgramGenerator.py`, `backend/app.py`, `backend/DatabaseConnection.py`,
`frontend/.../interfaces.ts`, `user-menu`, `admin-menu`, νέο `components/results-dialog`

### Αιτία

Για να δούμε το αποτέλεσμα ενός pipeline, έπρεπε να διαβάσουμε την έξοδο του `show()` από
το cmd (`docker logs executor` ή `kubectl logs`).

### Λύση

1. **Το πρόγραμμα γράφει μια προεπισκόπηση.** Όταν δίνεται `results_path`, το παραγόμενο
   πρόγραμμα αποθηκεύει στο τέλος τις πρώτες 50 γραμμές του τελικού DataFrame, μαζί με το
   πλήθος όλων των γραμμών, ως JSON στο `/mnt/shared/results/<id>-<όνομα>.json`. Τα
   διανύσματα γίνονται πίνακες αριθμών· τα μεγάλα αραιά διανύσματα (π.χ. TF-IDF με 1024
   θέσεις) αποθηκεύονται μόνο με τις μη μηδενικές θέσεις τους. Το αρχείο γράφεται πρώτα
   με προσωρινό όνομα και μετονομάζεται, ώστε να μη διαβαστεί ποτέ μισό.
2. **Νέο endpoint `GET /job-results/<job_id>`** (με `@authentication_required`): επιστρέφει
   την προεπισκόπηση, αν υπάρχει, και το πιο πρόσφατο log του job από τη συλλογή `Logs`
   (νέα `retrieve_latest_log`). Η διαδρομή του JSON προκύπτει από το executable του job,
   και διαβάζονται μόνο αρχεία μέσα στο `results/` του κοινού φακέλου.
3. **UI:** στη στήλη Status των πινάκων χρήστη και διαχειριστή εμφανίζεται κουμπί για τα
   jobs `finished` (πίνακας) και `failed` (log). Ανοίγει παράθυρο με δύο όψεις, **Result**
   (πίνακας) και **Log**.

Το παράθυρο χρησιμοποιεί μόνο components που ήδη υπήρχαν στο bundle, χωρίς tabs ή
spinner, γιατί η production build του frontend ήταν ήδη στα 998 kB, πολύ κοντά στο όριο
του 1 MB (`angular.json` → `budgets`). Με την πρώτη εκδοχή (με `MatTabs`) έφτανε τα
1,03 MB και η build αποτύγχανε. Η τελική είναι 1005 kB, εντός ορίου.

---

## [026] 2026-10-20 — Επαλήθευση των [021]–[025] εκτός Docker

**Τύπος:** Έλεγχος λειτουργίας (καμία αλλαγή κώδικα)

Οι δοκιμές έγιναν σε ξεχωριστό περιβάλλον, με PySpark 3.5.0 και numpy 1.24.4 όπως στις
εικόνες, όχι στο Docker Desktop.

| Έλεγχος | Αποτέλεσμα |
|---------|-----------|
| Πέντε pipelines παρήχθησαν και εκτελέστηκαν πραγματικά με Spark: K-Means → Random Forest, PCA (standard) → K-Means στο `pca_1`, Chi-Squared → Logistic Regression στο `selected_1`, TF-IDF → Logistic Regression (έξοδος Parquet), Word2Vec → K-Means (έξοδος CSV) | Όλα ολοκληρώθηκαν (exit 0) και έγραψαν σωστό JSON προεπισκόπησης |
| Έξοδος CSV με εξαγόμενα διανύσματα | Γράφτηκαν ως `"[0.031, -0.016, …]"` |
| Μη έγκυρες προδιαγραφές (δύο στήλες κειμένου, κανονικοποίηση σε κείμενο, Chi-Squared χωρίς label) | Απορρίφθηκαν με σαφές μήνυμα |
| `build_job_options` + `SparkCommandGenerator` για Kubernetes | Σωστή εντολή `spark-submit` (master, hostPath για driver και executors, όνομα pod, `local://` executable) |
| `/job-results/<id>` (Flask test client) | Επιστρέφει προεπισκόπηση και log· 404 για άγνωστο job· δεν διαβάζει αρχεία εκτός `results/` |
| Λογική επιτυχίας του executor, με προσομοιωμένα `kubectl`/`spark-submit` | `Succeeded` → finished, `Failed` ή pod που δεν δημιουργήθηκε → failed με log |
| Production build του frontend | Επιτυχής, 1005 kB |

**Δεν έχει δοκιμαστεί ακόμα:** η εκτέλεση στο ίδιο το Docker Desktop, δηλαδή το hostPath σε
πραγματικά pods, η εγγραφή του JSON από τον χρήστη `spark` (uid 185) στον φάκελο των
Windows και το `kubectl logs` μέσα από τον executor. Αυτό είναι το επόμενο βήμα.

---

## [027] 2026-10-21 — Πρώτη εκτέλεση σε pods: δικαιώματα εγγραφής στον κοινό φάκελο

**Τύπος:** Τροποποίηση των `spark-image/Dockerfile` και `backend/SparkProgramGenerator.py`

### Αποτέλεσμα της δοκιμής

Δύο εκτελέσεις του pipeline `test-kmeans` (K-Means, `age, income`, Min-Max, k = 2) στο
Kubernetes του Docker Desktop:

1. **Πρώτη:** απέτυχε με `PATH_NOT_FOUND`, επειδή η διαδρομή εισόδου γράφτηκε στη φόρμα
   χωρίς την αρχική `/` (`mnt/shared/...`) και το Spark την έψαξε σχετικά με τον φάκελο
   εργασίας του pod. Δεν πρόκειται για σφάλμα του κώδικα.
2. **Δεύτερη** (με `/mnt/shared/data/customers.csv`): ο υπολογισμός ολοκληρώθηκε. Το driver
   pod και το executor pod σηκώθηκαν με την εικόνα `spark-ml:3.5.0`, το volume `shared`
   (hostPath) προσαρτήθηκε, και το πρόγραμμα και τα δεδομένα διαβάστηκαν από αυτό. Στο
   τέλος όμως ο driver απέτυχε:

```
PermissionError: [Errno 13] Permission denied: '/mnt/shared/results/34335cc0f0c4-test-kmeans.json.tmp'
```

Επιβεβαιώνονται τα [021], [022] και [024]: το hostPath λειτουργεί για ανάγνωση, η εικόνα
έχει numpy, και ο executor αναγνώρισε σωστά την αποτυχία και αποθήκευσε το log του pod.

### Αιτία

Η εικόνα `apache/spark` τρέχει ως χρήστης `spark` (uid 185). Ο φάκελος των Windows φαίνεται
στον κόμβο του Kubernetes ως ιδιοκτησία του root, οπότε ο uid 185 μπορεί να διαβάσει αλλά
όχι να γράψει. Το ίδιο θα συνέβαινε και με ένα output path μέσα στο `/mnt/shared`.

### Αλλαγές

| Αρχείο | Αλλαγή | Σκοπός |
|--------|--------|--------|
| `spark-image/Dockerfile` | Αφαίρεση της τελικής `USER spark`: τα pods τρέχουν ως root | Δικαίωμα εγγραφής στον κοινό φάκελο. Αποδεκτό για το τοπικό cluster δοκιμών. Σε κανονικό cluster, ο φάκελος του hostPath πρέπει να γίνει εγγράψιμος για τον uid 185. |
| `backend/SparkProgramGenerator.py` | Η κλήση `save_preview` μπήκε σε `try/except OSError`, που τυπώνει προειδοποίηση | Αν η προεπισκόπηση δεν μπορεί να γραφτεί, το pipeline δεν πρέπει να σημειώνεται ως αποτυχημένο, αφού η πραγματική δουλειά του έχει ήδη γίνει. |

Χρειάζεται νέο build της εικόνας: `docker compose --profile images build spark-ml`.

---

## [028] 2026-10-21 — Παράθυρο αποτελεσμάτων: φιλτράρισμα του log

**Τύπος:** Τροποποίηση του `frontend/src/app/components/results-dialog/results-dialog.component.{ts,html}`

### Αιτία

Μετά το [027] το job `test-kmeans` ολοκληρώθηκε (`finished`). Το παράθυρο όμως άνοιξε στο
log, γιατί δεν βρέθηκε αρχείο προεπισκόπησης: ο φάκελος `spark-shared/results/` ήταν άδειος,
άρα η εγγραφή απέτυχε ξανά και πιάστηκε από το `try/except` του [027]. Ο πίνακας του `show()`
υπήρχε μέσα στο log, αλλά χαμένος ανάμεσα σε εκατοντάδες γραμμές `INFO` του Spark.

### Αλλαγή

Η όψη Log δείχνει πλέον, από προεπιλογή, μόνο την **έξοδο του προγράμματος**: αφαιρούνται οι
γραμμές `INFO`/`WARN`/`DEBUG`/`TRACE` του Spark (μορφή `yy/MM/dd HH:mm:ss LEVEL ...`), μαζί
με τις εσοχές που τις συνεχίζουν, όπως τα μπλοκ κατάστασης του pod. Μένουν ο πίνακας του
`show()`, οι προειδοποιήσεις του προγράμματος και τα Python tracebacks. Ένα κουμπί
εναλλάσσει την προβολή στο πλήρες log.

Η αιτία της αποτυχίας εγγραφής διερευνάται ακόμα: είτε η εικόνα `spark-ml:3.5.0` δεν
ξαναχτίστηκε μετά το [027], είτε η εγγραφή αποτυγχάνει ακόμα και ως root.

---

## [029] 2026-10-21 — Η προεπισκόπηση γράφεται από τον executor, όχι από το driver pod

**Τύπος:** Τροποποίηση των `backend/SparkProgramGenerator.py`, `backend/app.py` και `executor/JobExecutor.py`

### Αιτία

Η εικόνα `spark-ml:3.5.0` είχε ξαναχτιστεί (`docker image inspect` → `User: root`). Παρ' όλα
αυτά, το driver pod εξακολουθούσε να μην μπορεί να γράψει στο `spark-shared/results/`. Άρα η
αλλαγή χρήστη του [027] δεν αρκεί. Η πιθανότερη εξήγηση, που δεν έχει ακόμα επιβεβαιωθεί,
είναι ότι το entrypoint της εικόνας `apache/spark` γυρίζει στον χρήστη `spark` ακόμα κι όταν
το container ξεκινά ως root. Ο container `executor`, αντίθετα, γράφει κανονικά στον ίδιο
φάκελο: δημιούργησε τους υποφακέλους και αντέγραψε εκεί τα δεδομένα δείγματος.

### Λύση

Το driver pod δεν γράφει πλέον κανένα αρχείο:

1. **Generator:** η `save_preview(data, path)` αντικαταστάθηκε από την `print_preview(data)`,
   που τυπώνει την προεπισκόπηση ως μία γραμμή JSON ανάμεσα σε δύο γραμμές-σημάδια
   (`=====SPARK-GUI-PREVIEW-BEGIN=====` / `...-END=====`). Η παράμετρος
   `generate_program(..., results_path=...)` έγινε `preview=True`. Το `try/except` του [027]
   αφαιρέθηκε, αφού η εκτύπωση δεν μπορεί να αποτύχει λόγω δικαιωμάτων.
2. **Executor:** νέες `results_path_of` και `store_preview`. Μετά την εκτέλεση, ο executor βρίσκει
   το JSON στο log (στο `kubectl logs` του driver ή, στις τοπικές εκτελέσεις, στο stdout),
   ελέγχει ότι είναι έγκυρο, το γράφει στο `/mnt/shared/results/<id>-<όνομα>.json` και το
   αφαιρεί από το log που αποθηκεύεται, αφήνοντας μια σύντομη σημείωση στη θέση του.
   Γράφει μόνο για προγράμματα που βρίσκονται μέσα στο `/mnt/shared/programs/`.
3. Το endpoint `/job-results` και το UI δεν αλλάζουν: διαβάζουν το ίδιο αρχείο από την ίδια θέση.

### Επαλήθευση (εκτός Docker)

Pipeline TF-IDF → Logistic Regression πάνω στο `reviews.csv`, που παρήχθη, εκτελέστηκε με
Spark 3.5.0 και πέρασε από την πραγματική `job_executor` (με προσομοιωμένη βάση): το job
βγήκε επιτυχημένο, το JSON γράφτηκε (16 από 16 γραμμές) και τα σημάδια δεν υπάρχουν στο
αποθηκευμένο log.

### Ανοιχτό ζήτημα

Το ίδιο πρόβλημα δικαιωμάτων αφορά και ένα **Output Path** μέσα στο `/mnt/shared`, γιατί εκεί
γράφει το ίδιο το Spark, από τον driver και τους executors. Μέχρι να λυθεί, το Output Path
πρέπει να μένει κενό στις εκτελέσεις σε Kubernetes. Η αλλαγή χρήστη στο `spark-image/Dockerfile`
([027]) διατηρείται μέχρι να επιβεβαιωθεί η αιτία.

---

## [030] 2026-10-21 — Έλεγχος ότι οι διαδρομές δεδομένων είναι απόλυτες

**Τύπος:** Τροποποίηση του `backend/SparkProgramGenerator.py` (`validate_spec`)

### Αιτία

Το pipeline `test-pca-chain` (Filter → PCA → K-Means → Decision Tree) απέτυχε για τον ίδιο
λόγο με την πρώτη δοκιμή του [027]: η διαδρομή εισόδου γράφτηκε χωρίς την αρχική `/`
(`mnt/shared/data/customers.csv`). Το Spark τη διάβασε ως σχετική ως προς τον φάκελο εργασίας
του pod και απέτυχε με `PATH_NOT_FOUND`, αφού όμως το job είχε ήδη υποβληθεί στο cluster.
Το ίδιο το πρόγραμμα, εκτελεσμένο με σωστή διαδρομή εκτός Docker, ολοκληρώνεται κανονικά.

### Αλλαγή

Νέα `_validate_path`: τα `input.path` και `output.path` πρέπει να είναι απόλυτες διαδρομές
(να αρχίζουν με `/`) ή URI με σχήμα (π.χ. `s3a://`, `hdfs://`). Διαφορετικά, η προδιαγραφή
απορρίπτεται πριν από οποιαδήποτε υποβολή, με μήνυμα που προτείνει τη σωστή μορφή:

```
input.path: 'mnt/shared/data/customers.csv' is not an absolute path - did you mean '/mnt/shared/data/customers.csv'?
```

Το μήνυμα εμφανίζεται στη φόρμα, τόσο στο «Preview» όσο και στο «Create pipeline».

---

## [031] 2026-10-21 — Έλεγχοι εισόδου πριν από την υποβολή

**Τύπος:** Τροποποίηση των `backend/app.py` και `backend/SparkProgramGenerator.py`

### Αιτία

Το pipeline `test-chisq` (Drop nulls → Chi-Squared → Random Forest → Linear Regression)
απέτυχε στο cluster. Το πρόγραμμα ήταν σωστό, αλλά:

1. η διαδρομή εισόδου ήταν `/mn/shared/data/customers.csv` (λείπει το `t`). Είναι απόλυτη,
   οπότε πέρασε τον έλεγχο του [030], αλλά δεν υπάρχει·
2. το στάδιο Linear Regression είχε ετικέτα `income` και στα χαρακτηριστικά του `age, income`.
   Αυτό δεν προκαλεί σφάλμα, αλλά το μοντέλο απλώς αντιγράφει την απάντηση από την είσοδό του.

### Αλλαγές

| Αρχείο | Έλεγχος | Μήνυμα |
|--------|---------|--------|
| `app.py` (νέα `missing_shared_input`) | Σε εκτέλεση Kubernetes, μια τοπική διαδρομή πρέπει να βρίσκεται μέσα στο `/mnt/shared/`, τον μόνο φάκελο που βλέπουν τα pods. Τα URI (π.χ. `s3a://`) και οι τοπικές εκτελέσεις εξαιρούνται. | `... is outside the shared folder - Kubernetes pods can only read files under /mnt/shared/` |
| `app.py` | Μια διαδρομή μέσα στο `/mnt/shared/` πρέπει να υπάρχει. Ο container `api` βλέπει τον ίδιο φάκελο. | `... does not exist in the shared folder` |
| `SparkProgramGenerator.py` | Η στήλη ετικέτας ενός σταδίου δεν επιτρέπεται να υπάρχει και στα χαρακτηριστικά του. | `stages[N].features: must not contain the label column 'income'` |

Οι έλεγχοι τρέχουν τόσο στο «Preview» όσο και στο «Create pipeline». Στο «Preview» η φόρμα
δεν στέλνει τον στόχο εκτέλεσης, οπότε ελέγχεται μόνο η ύπαρξη του αρχείου. Δοκιμάστηκαν με
τον test client του Flask: σωστή διαδρομή → 200, `/mn/shared/...` → 400, ανύπαρκτο αρχείο →
400, `/data/customers.csv` σε τοπική εκτέλεση → 200, `s3a://...` → 200, ετικέτα στα
χαρακτηριστικά → 400.
