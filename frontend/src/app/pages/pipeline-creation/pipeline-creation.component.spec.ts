import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PipelineCreationComponent } from './pipeline-creation.component';

describe('PipelineCreationComponent', () => {
  let component: PipelineCreationComponent;
  let fixture: ComponentFixture<PipelineCreationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PipelineCreationComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(PipelineCreationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
