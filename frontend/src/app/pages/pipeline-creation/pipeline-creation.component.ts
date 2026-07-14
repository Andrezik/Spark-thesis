import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { CommonModule } from '@angular/common';
import { NgZone } from '@angular/core';

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
    MatIconModule
  ],
  templateUrl: './pipeline-creation.component.html',
  styleUrl: './pipeline-creation.component.css'
})
export class PipelineCreationComponent implements OnInit {
  pipelineForm!: FormGroup;
  user: string = '';
  fileName: string = '';
  selectedFile: File | null = null;
  pipelineCreated: boolean = false;
  pipelineError: boolean = false;
  noFileError: boolean = false;

  constructor(private fb: FormBuilder, private router: Router, private http: HttpClient, private ngZone: NgZone) {}

  ngOnInit() {
    this.user = localStorage.getItem('username') || '';
    this.pipelineForm = this.fb.group({
      name: ['', Validators.required],
      algorithm: ['', Validators.required],
      instances: [1, Validators.required],
      cores: [1, Validators.required],
      memory: [512, Validators.required],
      image: ['apache/spark:3.5.0', Validators.required]
    });
  }

  goBack() {
    this.router.navigate(['user-menu']);
  }

  goDashboard() {
  window.location.href = 'http://localhost:8080/#/user-menu';
  } 

  logout() {
    localStorage.clear();
    this.router.navigate(['login']);
  }

  onFileSelected(event: any) {
    const file = event.target.files[0];
    if (file) {
      this.selectedFile = file;
      this.fileName = file.name;
    }
  }

  submitPipeline() {
  if (this.pipelineForm.valid && this.selectedFile) {
    const form = this.pipelineForm.value;
    const ticket = localStorage.getItem('ticket') || '';
    const headers = new HttpHeaders({ 'Ticket': ticket });

    const formData = new FormData();
    formData.append('spark_exe', this.selectedFile, this.selectedFile.name);
    formData.append('user', this.user);

    this.http.post<any>('http://localhost:5000/upload_executable', formData, { headers }).subscribe({
      next: (uploadRes) => {
        const dataPath = uploadRes.file_path;
        const job = {
          username: this.user,
          status: 'pending',
          priority: 1,
          additionalOptions: [],
          options: {
            sparkPath: '/opt/spark/bin/spark-submit',
            master: 'k8s://https://kubernetes.default.svc',
            deployMode: 'cluster',
            name: form.name,
            class: form.algorithm,
            executable: dataPath,
            conf: {
              'spark.executor.instances': form.instances,
              'spark.executor.cores': form.cores,
              'spark.executor.memory': form.memory,
              'spark.kubernetes.container.image': form.image,
              'spark.kubernetes.driver.node.selector.kubernetes.io/hostname': 'k8s-1',
              'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark'
            }
          }
        };
        this.http.post('http://localhost:5000/spark-job', job, { headers }).subscribe({
          next: () => {
            this.pipelineCreated = true;
          },
          error: () => {
            this.pipelineError = true;
          }
        });
      },
      error: () => {
        this.pipelineError = true;
      }
    });
  } else if (!this.selectedFile) {
    this.noFileError = true;
  }
}
}