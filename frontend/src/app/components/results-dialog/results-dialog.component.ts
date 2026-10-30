import {Component, Inject, OnInit} from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {NgForOf, NgIf} from "@angular/common";
import {MatButtonModule} from "@angular/material/button";
import {
  MAT_DIALOG_DATA,
  MatDialogActions,
  MatDialogClose,
  MatDialogContent,
  MatDialogTitle
} from "@angular/material/dialog";
import {JobResults} from "../../interfaces";

/** A line of Spark's own logging, e.g. "26/10/21 12:02:50 INFO DAGScheduler: ..." */
const SPARK_LOG_LINE = /^\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2} (INFO|WARN|DEBUG|TRACE) /;

/**
 * Shows the outcome of a finished (or failed) job: the first rows of the
 * pipeline's result as a table, and the log of the run (driver output).
 * Both come from GET /job-results/<job_id>.
 */
@Component({
  selector: 'app-results-dialog',
  standalone: true,
  imports: [MatButtonModule, MatDialogActions, MatDialogClose, MatDialogTitle, MatDialogContent,
    NgForOf, NgIf],
  templateUrl: './results-dialog.component.html',
  styleUrl: './results-dialog.component.css'
})
export class ResultsDialogComponent implements OnInit {
  loading = true;
  errorMessage = '';
  results: JobResults | null = null;
  /** Which part is shown: the result table or the log of the run */
  view: 'result' | 'log' = 'result';
  /** The log without Spark's INFO/WARN lines: what the program itself printed, and errors */
  programOutput = '';
  /** Show the whole log instead of programOutput */
  showFullLog = false;

  constructor(@Inject(MAT_DIALOG_DATA) public data: { jobId: string, status: string },
              private http: HttpClient) {
  }

  ngOnInit() {
    const ticket = localStorage.getItem('ticket') || '';
    this.http.get<JobResults>('http://localhost:5000/job-results/' + this.data.jobId,
      {headers: {"ticket": ticket}, responseType: 'json'}).subscribe({
      next: response => {
        this.loading = false;
        this.results = response;
        this.programOutput = this.withoutSparkLogging(response.log || '');
        // A job without a result table (e.g. a failed one) opens on its log
        this.view = response.preview ? 'result' : 'log';
      },
      error: err => {
        this.loading = false;
        this.errorMessage = err?.error?.message || 'Could not load the results of this job.';
      }
    });
  }

  /**
   * Drops Spark's own INFO/WARN lines, together with the indented lines that
   * continue them (e.g. the pod status blocks). What is left is the program's
   * output: the table printed by show(), warnings and Python tracebacks.
   */
  private withoutSparkLogging(log: string): string {
    const kept: string[] = [];
    let skipping = false;
    for (const line of log.split('\n')) {
      if (SPARK_LOG_LINE.test(line)) {
        skipping = true;
        continue;
      }
      if (skipping && /^\s/.test(line)) {
        continue;
      }
      skipping = false;
      kept.push(line);
    }
    // Collapse the empty lines left behind
    return kept.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  }

  /**
   * Vectors arrive as arrays of numbers, long sparse vectors as
   * {size, nonzero: {index: value}}; everything else is shown as is.
   */
  format(value: any): string {
    if (value === null || value === undefined) {
      return '';
    }
    if (Array.isArray(value)) {
      return '[' + value.map(item => this.format(item)).join(', ') + ']';
    }
    if (typeof value === 'object' && value.nonzero) {
      const entries = Object.entries(value.nonzero).map(([index, item]) => `${index}: ${item}`);
      return `(size ${value.size}) {${entries.join(', ')}}`;
    }
    return String(value);
  }
}
