export interface ErrorResponse {
  status: "error";
  message: string;
}

export interface AuthenticationResponse {
  status: "success";
  rights: string;
  username: string;
  ticket: string;
}

export interface AuthenticationRequest {
  username: string;
  password: string;
}

export interface StatusResponse {
  status: string;
  message: string;
}

export interface JobCheck {
  messages: string[];
}

export interface JobElement {
  position: number;
  job_id: string;
  status: string;
  user: string;
  priority: number;
  timestamp: string;
  job: Options;
}

export interface Jobs {
  [index: number]: {
    id: string;
    status: string;
    user: string;
    priority: number;
    timestamp: string;
    options: Options;
  };
}

export interface Job {
  id: string;
  status: string;
  user: string;
  timestamp: string;
  options: Options;
  [key: string]: any;
}

export interface Options {
  sparkPath: string;
  master: string;
  deployMode: string;
  name: string;
  class: string;
  conf: Configurations;
  executable: string;
}

export interface Configurations {
  [confKey: string]: string | number;
  // spark_executor_instances: number;
  // "spark.executor.cores": number;
  // spark_executor_memory: string;
  // spark_kubernetes_container_image: string;
  // spark_kubernetes_driver_node_selector_kubernetes_io_hostname: string;
  // spark_kubernetes_authenticate_driver_serviceAccountName: string;
}

export interface NewJob {
  username: string;
  status: string;
  options: Options;
  [key: string]: any;
}

/** Response of GET /job-results/<job_id> */
export interface JobResults {
  status: string;
  job_status: string;
  name: string;
  /** First rows of the pipeline result; null for jobs that wrote none */
  preview: {
    columns: string[];
    rows: any[][];
    total_rows: number;
    preview_rows: number;
  } | null;
  log: string | null;
  log_timestamp: string | null;
}
