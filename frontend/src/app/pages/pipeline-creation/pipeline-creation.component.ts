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
interface AlgorithmDefinition {
  value: string;
  label: string;
  labelRequired: boolean;
  params: AlgorithmParam[];
}

const ALGORITHMS: AlgorithmDefinition[] = [
  {
    value: 'kmeans',
    label: 'K-Means (clustering)',
    labelRequired: false,
    params: [
      { name: 'k', label: 'Number of clusters (k)', default: 3 },
      { name: 'maxIter', label: 'Max iterations', default: 20 },
      { name: 'seed', label: 'Random seed', default: 1 }
    ]
  },
  {
    value: 'logistic_regression',
    label: 'Logistic Regression (classification)',
    labelRequired: true,
    params: [
      { name: 'maxIter', label: 'Max iterations', default: 10 },
      { name: 'regParam', label: 'Regularization parameter', default: 0 }
    ]
  },
  {
    value: 'linear_regression',
    label: 'Linear Regression (regression)',
    labelRequired: true,
    params: [
      { name: 'maxIter', label: 'Max iterations', default: 10 },
      { name: 'regParam', label: 'Regularization parameter', default: 0 }
    ]
  },
  {
    value: 'decision_tree',
    label: 'Decision Tree (classification)',
    labelRequired: true,
    params: [
      { name: 'maxDepth', label: 'Max depth', default: 5 }
    ]
  }
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
  formats = ['csv', 'json', 'parquet', 'orc', 'text'];
  writeModes = ['overwrite', 'append', 'ignore', 'error'];
  // Where the generated program is executed. 'kubernetes' is the original
  // behaviour; 'local' runs it with Spark inside the executor container and
  // needs no cluster.
  targets = [
    { value: 'kubernetes', label: 'Kubernetes cluster' },
    { value: 'local', label: 'Local (inside the executor)' }
  ];
  stepTypes = [
    { value: 'select', label: 'Select columns' },
    { value: 'filter', label: 'Filter rows' },
    { value: 'dropna', label: 'Drop rows with nulls' },
    { value: 'limit', label: 'Limit rows' }
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

      // --- the algorithm to apply ---
      algorithm: ['kmeans', Validators.required],
      features: ['', Validators.required],
      label: [''],
      params: this.fb.group({}),

      // --- where the result goes ---
      outputPath: [''],
      outputFormat: ['parquet'],
      outputMode: ['overwrite'],

      // --- execution target and cluster resources ---
      target: ['kubernetes', Validators.required],
      instances: [1, Validators.required],
      cores: [1, Validators.required],
      memory: [512, Validators.required],
      image: ['apache/spark:3.5.0', Validators.required]
    });

    // Build the parameter controls for the initially selected algorithm, and
    // rebuild them whenever the user picks a different one.
    this.rebuildParamControls('kmeans');
    this.pipelineForm.get('algorithm')!.valueChanges.subscribe((value: string) => {
      this.rebuildParamControls(value);
    });
  }

  /* ------------------------------------------------------------------ *
   *  Algorithm-driven controls
   * ------------------------------------------------------------------ */

  get selectedAlgorithm(): AlgorithmDefinition | undefined {
    return this.algorithms.find(a => a.value === this.pipelineForm?.get('algorithm')?.value);
  }

  get labelRequired(): boolean {
    return this.selectedAlgorithm?.labelRequired === true;
  }

  get paramsGroup(): FormGroup {
    return this.pipelineForm.get('params') as FormGroup;
  }

  /** Replaces the params sub-form with the controls the chosen algorithm needs. */
  private rebuildParamControls(algorithmValue: string) {
    const group = this.paramsGroup;
    Object.keys(group.controls).forEach(key => group.removeControl(key));

    const definition = this.algorithms.find(a => a.value === algorithmValue);
    definition?.params.forEach(param => {
      group.addControl(param.name, this.fb.control(param.default, Validators.required));
    });

    // The label column only applies to supervised algorithms
    const labelControl = this.pipelineForm.get('label')!;
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
      n: [1000]
    }));
  }

  removeStep(index: number) {
    this.steps.removeAt(index);
  }

  stepType(index: number): string {
    return this.steps.at(index).get('type')!.value;
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
      algorithm: {
        name: form.algorithm,
        features: this.splitColumns(form.features),
        params: { ...form.params }
      }
    };

    // Reader options only make sense for text-based formats
    if (form.inputFormat === 'csv') {
      spec.input.options = { header: form.header, inferSchema: form.inferSchema };
    }

    // Transformation steps, keeping only the field each type actually uses
    for (const step of form.steps) {
      if (step.type === 'select') {
        spec.steps.push({ type: 'select', columns: this.splitColumns(step.columns) });
      } else if (step.type === 'filter') {
        spec.steps.push({ type: 'filter', condition: step.condition });
      } else if (step.type === 'dropna') {
        spec.steps.push({ type: 'dropna' });
      } else if (step.type === 'limit') {
        spec.steps.push({ type: 'limit', n: Number(step.n) });
      }
    }

    if (this.labelRequired) {
      spec.algorithm.label = form.label;
    }

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
