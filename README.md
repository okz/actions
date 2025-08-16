# ice-stream
## Minimal-data excluded variables

These variables are omitted from the minimal export profile:

| Variable              | Dimensions                                                       | Dtype   |
|-----------------------|------------------------------------------------------------------|---------|
| bearing               | (high_res_timestamp)                                             | float64 |
| covariance            | (timestamp, fitted_measurement, fitted_measurement_duplicate)    | float64 |
| diagnostics_am_scale  | (timestamp, diagnostics_am_scale_sample)                         | float64 |
| diagnostics_i         | (timestamp, diagnostic_sample)                                   | float64 |
| diagnostics_q         | (timestamp, diagnostic_sample)                                   | float64 |
| sonictemp             | (high_res_timestamp)                                             | float64 |
| speed                 | (high_res_timestamp)                                             | float64 |
| waveforms_am          | (timestamp, sample)                                              | float64 |
| waveforms_fm          | (timestamp, sample)                                              | float64 |
| waveforms_phase       | (timestamp, sample)                                              | float64 |
| waveforms_residuals   | (timestamp, sample)                                              | float64 |
| waveforms_wavenumbers | (timestamp, sample)                                              | float64 |
| windx                 | (high_res_timestamp)                                             | float64 |
| windy                 | (high_res_timestamp)                                             | float64 |
| windz                 | (high_res_timestamp)                                             | float64 |
