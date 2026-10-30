import { Component, OnInit } from '@angular/core';
import { FormArray, FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { CommonModule } from '@angular/common';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';

/**
 * Description of one algorithm parameter, used to build the form controls
 * dynamically when the user picks a different algorithm.
 */
interface AlgorithmParam {
  name: string;
  label: string;
  default: number;
}

/**
 * Front-end mirror of the ALGORITHMS catalogue in backend/SparkProgramGenerator.py.
 * Keeping the two in sync means the form only ever offers combinations the
 * backend can actually generate.
 */
type CategoryValue = 'clustering' | 'classification' | 'regression' | 'feature_extraction';

interface AlgorithmDefinition {
  value: string;
  label: string;
  category: CategoryValue;
  labelRequired: boolean;
  /** 'text': exactly one text column instead of numeric feature columns (TF-IDF, Word2Vec) */
  input?: 'vector' | 'text';
  /** Feature extraction only: base name of the column the stage adds ('pca' -> 'pca_1') */
  outputCol?: string;
  params: AlgorithmParam[];
}

interface AlgorithmCategory {
  value: CategoryValue;
  label: string;
}

const ALGORITHMS: AlgorithmDefinition[] = [
  // --- clustering ---
  {
    value: 'kmeans',
    label: 'K-Means',
    category: 'clustering',
    labelRequired: false,
    params: [
      { name: 'k', label: 'Number of clusters (k)', default: 3 },
      { name: 'maxIter', label: 'Max iterations', default: 20 },
      { name: 'seed', label: 'Random seed', default: 1 }
    ]
  },
  {
    value: 'bisecting_kmeans',
    label: 'Bisecting K-Means',
    category: 'clustering',
    labelRequired: false,
    params: [
      { name: 'k', label: 'Number of clusters (k)', default: 4 },
      { name: 'maxIter', label: 'Max iterations', default: 20 },
      { name: 'seed', label: 'Random seed', default: 1 }
    ]
  },
  {
    value: 'gaussian_mixture',
    label: 'Gaussian Mixture',
    category: 'clustering',
    labelRequired: false,
    params: [
      { name: 'k', label: 'Number of components (k)', default: 2 },
      { name: 'maxIter', label: 'Max iterations', default: 100 },
      { name: 'seed', label: 'Random seed', default: 1 }
    ]
  },
  // --- classification ---
  {
    value: 'logistic_regression',
    label: 'Logistic Regression',
    category: 'classification',
    labelRequired: true,
    params: [
      { name: 'maxIter', label: 'Max iterations', default: 10 },
      { name: 'regParam', label: 'Regularization parameter', default: 0 }
    ]
  },
  {
    value: 'decision_tree',
    label: 'Decision Tree',
    category: 'classification',
    labelRequired: true,
    params: [
      { name: 'maxDepth', label: 'Max depth', default: 5 }
    ]
  },
  {
    value: 'random_forest',
    label: 'Random Forest',
    category: 'classification',
    labelRequired: true,
    params: [
      { name: 'numTrees', label: 'Number of trees', default: 20 },
      { name: 'maxDepth', label: 'Max depth', default: 5 },
      { name: 'seed', label: 'Random seed', default: 1 }
    ]
  },
  // --- regression ---
  {
    value: 'linear_regression',
    label: 'Linear Regression',
    category: 'regression',
    labelRequired: true,
    params: [
      { name: 'maxIter', label: 'Max iterations', default: 10 },
      { name: 'regParam', label: 'Regularization parameter', default: 0 }
    ]
  },
  {
    value: 'decision_tree_regressor',
    label: 'Decision Tree Regressor',
    category: 'regression',
    labelRequired: true,
    params: [
      { name: 'maxDepth', label: 'Max depth', default: 5 }
    ]
  },
  // --- feature extraction ---
  {
    value: 'pca',
    label: 'PCA (Principal Component Analysis)',
    category: 'feature_extraction',
    labelRequired: false,
    input: 'vector',
    outputCol: 'pca',
    params: [
      { name: 'k', label: 'Number of components (k)', default: 2 }
    ]
  },
  {
    value: 'chisq_selector',
    label: 'Chi-Squared feature selection',
    category: 'feature_extraction',
    labelRequired: true,
    input: 'vector',
    outputCol: 'selected',
    params: [
      { name: 'numTopFeatures', label: 'Features to keep', default: 2 }
    ]
  },
  {
    value: 'tf_idf',
    label: 'TF-IDF (text)',
    category: 'feature_extraction',
    labelRequired: false,
    input: 'text',
    outputCol: 'tfidf',
    params: [
      { name: 'numFeatures', label: 'Number of hash buckets', default: 1024 },
      { name: 'minDocFreq', label: 'Min document frequency', default: 0 }
    ]
  },
  {
    value: 'word2vec',
    label: 'Word2Vec (text)',
    category: 'feature_extraction',
    labelRequired: false,
    input: 'text',
    outputCol: 'w2v',
    params: [
      { name: 'vectorSize', label: 'Vector size', default: 10 },
      { name: 'minCount', label: 'Min word count', default: 1 },
      { name: 'seed', label: 'Random seed', default: 1 }
    ]
  }
];

/** Order and display names of the categories shown in the first dropdown of a stage. */
const CATEGORIES: AlgorithmCategory[] = [
  { value: 'clustering', label: 'Clustering' },
  { value: 'classification', label: 'Classification' },
  { value: 'regression', label: 'Regression' },
  { value: 'feature_extraction', label: 'Feature Extraction' }
];

const BACKEND_URL = 'http://localhost:5000';

@Component({
  selector: 'app-pipeline-creation',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatToolbarModule,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatCheckboxModule,
    MatTooltipModule,
    MatDividerModule
  ],
  templateUrl: './pipeline-creation.component.html',
  styleUrl: './pipeline-creation.component.css'
})
export class PipelineCreationComponent implements OnInit {
  pipelineForm!: FormGroup;
  user: string = '';

  algorithms = ALGORITHMS;
  categories = CATEGORIES;
  formats = ['csv', 'json', 'parquet', 'orc', 'text'];
  writeModes = ['overwrite', 'append', 'ignore', 'error'];
  scalers = [
    { value: '', label: 'None' },
    { value: 'minmax', label: 'Min-Max scaling (0 to 1)' },
    { value: 'standard', label: 'Standard scaling (zero mean, unit variance)' }
  ];
  stepTypes = [
    { value: 'select', label: 'Select columns' },
    { value: 'filter', label: 'Filter rows' },
    { value: 'dropna', label: 'Drop rows with nulls' },
    { value: 'limit', label: 'Limit rows' },
    { value: 'string_indexer', label: 'String Indexer (text to numeric)' },
    { value: 'one_hot_encoder', label: 'One-Hot Encoder (numeric to vector)' }
  ];
  // Where the generated program is executed. 'kubernetes' is the original
  // behaviour; 'local' runs it with Spark inside the executor container and
  // needs no cluster.
  targets = [
    { value: 'kubernetes', label: 'Kubernetes cluster' },
    { value: 'local', label: 'Local (inside the executor)' }
  ];

  // Feedback shown to the user
  generatedProgram: string = '';
  successMessage: string = '';
  errorMessage: string = '';
  validationErrors: string[] = [];
  busy: boolean = false;

  constructor(private fb: FormBuilder, private router: Router, private http: HttpClient) {}

  ngOnInit() {
    this.user = localStorage.getItem('username') || '';

    this.pipelineForm = this.fb.group({
      name: ['', Validators.required],

      // --- where the data comes from ---
      inputPath: ['', Validators.required],
      inputFormat: ['csv', Validators.required],
      header: [true],
      inferSchema: [true],

      // --- optional transformations, added by the user ---
      steps: this.fb.array([]),

      // --- one or more algorithm stages, applied in order ---
      stages: this.fb.array([]),

      // --- where the result goes ---
      outputPath: [''],
      outputFormat: ['parquet'],
      outputMode: ['overwrite'],

      // --- execution target and cluster resources ---
      target: ['kubernetes', Validators.required],
      instances: [1, Validators.required],
      cores: [1, Validators.required],
      memory: [512, Validators.required],
      // apache/spark plus numpy (see spark-image/Dockerfile); pyspark.ml needs numpy
      image: ['spark-ml:3.5.0', Validators.required]
    });

    // A pipeline always has at least one stage
    this.addStage();
  }

  /* ------------------------------------------------------------------ *
   *  Algorithm stages (dynamic list)
   * ------------------------------------------------------------------ */

  get stages(): FormArray {
    return this.pipelineForm.get('stages') as FormArray;
  }

  /** The algorithms belonging to one category, for the second dropdown of a stage. */
  algorithmsOf(category: string): AlgorithmDefinition[] {
    return this.algorithms.filter(algorithm => algorithm.category === category);
  }

  /** The algorithms offered for a stage: only those of the category picked for it. */
  stageAlgorithms(index: number): AlgorithmDefinition[] {
    return this.algorithmsOf(this.stages.at(index).get('category')!.value);
  }

  definitionOf(algorithmValue: string): AlgorithmDefinition | undefined {
    return this.algorithms.find(algorithm => algorithm.value === algorithmValue);
  }

  stageDefinition(index: number): AlgorithmDefinition | undefined {
    return this.definitionOf(this.stages.at(index).get('algorithm')!.value);
  }

  stageLabelRequired(index: number): boolean {
    return this.stageDefinition(index)?.labelRequired === true;
  }

  /** TF-IDF and Word2Vec read one text column; they get no feature scaling. */
  stageTextInput(index: number): boolean {
    return this.stageDefinition(index)?.input === 'text';
  }

  stageParams(index: number): FormGroup {
    return this.stages.at(index).get('params') as FormGroup;
  }

  /**
   * The column a stage adds that a later stage can feed on: 'prediction_N' for
   * the predicting algorithms, e.g. 'pca_N' for feature extraction. Mirrors
   * output_column() in backend/SparkProgramGenerator.py.
   */
  outputColumnOf(index: number): string {
    const definition = this.stageDefinition(index);
    const base = definition?.category === 'feature_extraction' ? definition.outputCol : 'prediction';
    return `${base}_${index + 1}`;
  }

  /**
   * Column names a stage can feed on: the columns produced by every earlier
   * stage. Shown as a hint so the user knows how to chain stages.
   */
  availableFrom(index: number): string[] {
    const columns: string[] = [];
    for (let i = 0; i < index; i++) {
      columns.push(this.outputColumnOf(i));
    }
    return columns;
  }

  addStage() {
    const group = this.fb.group({
      category: ['clustering' as CategoryValue, Validators.required],
      algorithm: ['kmeans', Validators.required],
      features: ['', Validators.required],
      label: [''],
      scaler: [''],
      params: this.fb.group({})
    });

    this.stages.push(group);

    // The stage is looked up when a value changes (not by a fixed index), so
    // removing an earlier stage cannot make these handlers update the wrong one
    this.rebuildParamControls(group, 'kmeans');
    group.get('algorithm')!.valueChanges.subscribe((value: string | null) => {
      this.rebuildParamControls(group, value || '');
    });
    // Picking a category offers only its algorithms; the first one is preselected
    group.get('category')!.valueChanges.subscribe((category: string | null) => {
      const first = this.algorithmsOf(category || '')[0];
      group.get('algorithm')!.setValue(first ? first.value : '');
    });
  }

  removeStage(index: number) {
    if (this.stages.length > 1) {
      this.stages.removeAt(index);
    }
  }

  /** Replaces a stage's params sub-form with the controls its algorithm needs. */
  private rebuildParamControls(stage: FormGroup, algorithmValue: string) {
    const group = stage.get('params') as FormGroup;
    Object.keys(group.controls).forEach(key => group.removeControl(key));

    const definition = this.definitionOf(algorithmValue);
    definition?.params.forEach(param => {
      group.addControl(param.name, this.fb.control(param.default, Validators.required));
    });

    // Scaling only applies to numeric features
    if (definition?.input === 'text') {
      stage.get('scaler')!.setValue('');
    }

    // The label column only applies to supervised algorithms
    const labelControl = stage.get('label')!;
    if (definition?.labelRequired) {
      labelControl.setValidators(Validators.required);
    } else {
      labelControl.clearValidators();
      labelControl.setValue('');
    }
    labelControl.updateValueAndValidity();
  }

  /* ------------------------------------------------------------------ *
   *  Transformation steps (dynamic list)
   * ------------------------------------------------------------------ */

  get steps(): FormArray {
    return this.pipelineForm.get('steps') as FormArray;
  }

  addStep() {
    this.steps.push(this.fb.group({
      type: ['select', Validators.required],
      columns: [''],
      condition: [''],
      n: [1000],
      inputCol: [''],
      outputCol: ['']
    }));
  }

  removeStep(index: number) {
    this.steps.removeAt(index);
  }

  stepType(index: number): string {
    return this.steps.at(index).get('type')!.value;
  }

  /** True for steps that map one column onto another (StringIndexer, OneHotEncoder). */
  isColumnTransformer(index: number): boolean {
    const type = this.stepType(index);
    return type === 'string_indexer' || type === 'one_hot_encoder';
  }

  /* ------------------------------------------------------------------ *
   *  Specification building
   * ------------------------------------------------------------------ */

  /** Turns "a, b , c" into ["a", "b", "c"], dropping empty entries. */
  private splitColumns(raw: string): string[] {
    return (raw || '')
      .split(',')
      .map(column => column.trim())
      .filter(column => column.length > 0);
  }

  /**
   * Builds the JSON specification that the backend turns into a PySpark program.
   * This is the object the professor asked the form to construct.
   */
  buildSpec(): any {
    const form = this.pipelineForm.value;

    const spec: any = {
      name: form.name,
      input: {
        path: form.inputPath,
        format: form.inputFormat,
        options: {}
      },
      steps: [],
      stages: []
    };

    // Reader options only make sense for text-based formats
    if (form.inputFormat === 'csv') {
      spec.input.options = { header: form.header, inferSchema: form.inferSchema };
    }

    // Transformation steps, keeping only the fields each type actually uses
    for (const step of form.steps) {
      if (step.type === 'select') {
        spec.steps.push({ type: 'select', columns: this.splitColumns(step.columns) });
      } else if (step.type === 'filter') {
        spec.steps.push({ type: 'filter', condition: step.condition });
      } else if (step.type === 'dropna') {
        spec.steps.push({ type: 'dropna' });
      } else if (step.type === 'limit') {
        spec.steps.push({ type: 'limit', n: Number(step.n) });
      } else if (step.type === 'string_indexer' || step.type === 'one_hot_encoder') {
        spec.steps.push({ type: step.type, inputCol: step.inputCol, outputCol: step.outputCol });
      }
    }

    // Algorithm stages, in the order the user arranged them
    form.stages.forEach((stage: any) => {
      const definition = this.definitionOf(stage.algorithm);
      const entry: any = {
        name: stage.algorithm,
        features: this.splitColumns(stage.features),
        params: { ...stage.params }
      };
      if (definition?.labelRequired) {
        entry.label = stage.label;
      }
      if (stage.scaler && definition?.input !== 'text') {
        entry.scaler = stage.scaler;
      }
      spec.stages.push(entry);
    });

    // Output is optional: with no path the generated program just shows the result
    if (form.outputPath) {
      spec.output = {
        path: form.outputPath,
        format: form.outputFormat,
        mode: form.outputMode
      };
    }

    return spec;
  }

  private buildResources(): any {
    const form = this.pipelineForm.value;
    return {
      target: form.target,
      instances: Number(form.instances),
      cores: Number(form.cores),
      memory: Number(form.memory),
      image: form.image
    };
  }

  get isLocalTarget(): boolean {
    return this.pipelineForm?.get('target')?.value === 'local';
  }

  private authHeaders(): HttpHeaders {
    return new HttpHeaders({ 'Ticket': localStorage.getItem('ticket') || '' });
  }

  private clearFeedback() {
    this.successMessage = '';
    this.errorMessage = '';
    this.validationErrors = [];
  }

  /** Reads the error payload the backend sends back on a 400. */
  private handleBackendError(error: any) {
    this.busy = false;
    const body = error?.error;
    if (body && body.errors) {
      this.errorMessage = body.message || 'Invalid pipeline specification';
      this.validationErrors = body.errors;
    } else {
      this.errorMessage = 'Could not reach the backend. Is the API running?';
    }
  }

  /* ------------------------------------------------------------------ *
   *  Actions
   * ------------------------------------------------------------------ */

  /** Asks the backend for the program this specification would produce, without storing anything. */
  previewProgram() {
    this.clearFeedback();
    this.generatedProgram = '';

    if (this.pipelineForm.invalid) {
      this.pipelineForm.markAllAsTouched();
      this.errorMessage = 'Please fill in all required fields.';
      return;
    }

    this.busy = true;
    this.http.post<any>(`${BACKEND_URL}/preview_program`, { spec: this.buildSpec() }, { headers: this.authHeaders() })
      .subscribe({
        next: response => {
          this.busy = false;
          this.generatedProgram = response.program;
        },
        error: error => this.handleBackendError(error)
      });
  }

  /** Sends the specification so the backend generates, stores and queues the program. */
  createPipeline() {
    this.clearFeedback();

    if (this.pipelineForm.invalid) {
      this.pipelineForm.markAllAsTouched();
      this.errorMessage = 'Please fill in all required fields.';
      return;
    }

    const payload = {
      username: this.user,
      spec: this.buildSpec(),
      resources: this.buildResources()
    };

    this.busy = true;
    this.http.post<any>(`${BACKEND_URL}/pipeline`, payload, { headers: this.authHeaders() })
      .subscribe({
        next: response => {
          this.busy = false;
          this.generatedProgram = response.program;
          this.successMessage = `Pipeline created. Generated program: ${response.filename}`;
        },
        error: error => this.handleBackendError(error)
      });
  }

  /* ------------------------------------------------------------------ *
   *  Navigation
   * ------------------------------------------------------------------ */

  goBack() {
    this.router.navigate(['user-menu']);
  }

  logout() {
    localStorage.clear();
    this.router.navigate(['login']);
  }
}
