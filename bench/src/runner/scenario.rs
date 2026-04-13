// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

/// Defines weighted operation distributions for benchmark scenarios.

#[derive(Debug, Clone)]
pub struct WeightedOp {
    pub op_type: OpType,
    pub weight: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    RawUpload,
    RawDownload,
    RawDelete,
    RawList,
    DockerPush,
    DockerPull,
}

impl OpType {
    pub fn name(&self) -> &'static str {
        match self {
            OpType::RawUpload => "raw-upload",
            OpType::RawDownload => "raw-download",
            OpType::RawDelete => "raw-delete",
            OpType::RawList => "raw-list",
            OpType::DockerPush => "docker-push",
            OpType::DockerPull => "docker-pull",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: String,
    pub ops: Vec<WeightedOp>,
    pub needs_raw_seed: bool,
    pub needs_docker_seed: bool,
}

pub fn get_scenarios(name: &str) -> Vec<Scenario> {
    match name {
        "raw-upload" => vec![Scenario {
            name: "raw-upload".into(),
            ops: vec![WeightedOp {
                op_type: OpType::RawUpload,
                weight: 100,
            }],
            needs_raw_seed: false,
            needs_docker_seed: false,
        }],
        "raw-download" => vec![Scenario {
            name: "raw-download".into(),
            ops: vec![WeightedOp {
                op_type: OpType::RawDownload,
                weight: 100,
            }],
            needs_raw_seed: true,
            needs_docker_seed: false,
        }],
        "raw-mixed" => vec![Scenario {
            name: "raw-mixed".into(),
            ops: vec![
                WeightedOp {
                    op_type: OpType::RawDownload,
                    weight: 60,
                },
                WeightedOp {
                    op_type: OpType::RawUpload,
                    weight: 30,
                },
                WeightedOp {
                    op_type: OpType::RawDelete,
                    weight: 5,
                },
                WeightedOp {
                    op_type: OpType::RawList,
                    weight: 5,
                },
            ],
            needs_raw_seed: true,
            needs_docker_seed: false,
        }],
        "docker-push" => vec![Scenario {
            name: "docker-push".into(),
            ops: vec![WeightedOp {
                op_type: OpType::DockerPush,
                weight: 100,
            }],
            needs_raw_seed: false,
            needs_docker_seed: false,
        }],
        "docker-pull" => vec![Scenario {
            name: "docker-pull".into(),
            ops: vec![WeightedOp {
                op_type: OpType::DockerPull,
                weight: 100,
            }],
            needs_raw_seed: false,
            needs_docker_seed: true,
        }],
        "docker-mixed" => vec![Scenario {
            name: "docker-mixed".into(),
            ops: vec![
                WeightedOp {
                    op_type: OpType::DockerPush,
                    weight: 50,
                },
                WeightedOp {
                    op_type: OpType::DockerPull,
                    weight: 50,
                },
            ],
            needs_raw_seed: false,
            needs_docker_seed: true,
        }],
        "all" => {
            let mut all = Vec::new();
            for s in &[
                "raw-upload",
                "raw-download",
                "raw-mixed",
                "docker-push",
                "docker-pull",
                "docker-mixed",
            ] {
                all.extend(get_scenarios(s));
            }
            all
        }
        _ => vec![],
    }
}

/// Select an operation type based on weighted random choice.
pub fn pick_op(ops: &[WeightedOp], rng_val: u32) -> OpType {
    let total: u32 = ops.iter().map(|o| o.weight).sum();
    let mut pick = rng_val % total;
    for op in ops {
        if pick < op.weight {
            return op.op_type;
        }
        pick -= op.weight;
    }
    ops.last().unwrap().op_type
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_type_names() {
        assert_eq!(OpType::RawUpload.name(), "raw-upload");
        assert_eq!(OpType::RawDownload.name(), "raw-download");
        assert_eq!(OpType::RawDelete.name(), "raw-delete");
        assert_eq!(OpType::RawList.name(), "raw-list");
        assert_eq!(OpType::DockerPush.name(), "docker-push");
        assert_eq!(OpType::DockerPull.name(), "docker-pull");
    }

    #[test]
    fn get_scenarios_raw_upload() {
        let scenarios = get_scenarios("raw-upload");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].name, "raw-upload");
        assert_eq!(scenarios[0].ops.len(), 1);
        assert_eq!(scenarios[0].ops[0].op_type, OpType::RawUpload);
        assert!(!scenarios[0].needs_raw_seed);
        assert!(!scenarios[0].needs_docker_seed);
    }

    #[test]
    fn get_scenarios_raw_download_needs_seed() {
        let scenarios = get_scenarios("raw-download");
        assert_eq!(scenarios.len(), 1);
        assert!(scenarios[0].needs_raw_seed);
        assert!(!scenarios[0].needs_docker_seed);
    }

    #[test]
    fn get_scenarios_raw_mixed_has_four_ops() {
        let scenarios = get_scenarios("raw-mixed");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].ops.len(), 4);
        let total_weight: u32 = scenarios[0].ops.iter().map(|o| o.weight).sum();
        assert_eq!(total_weight, 100);
    }

    #[test]
    fn get_scenarios_docker_push() {
        let scenarios = get_scenarios("docker-push");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].ops[0].op_type, OpType::DockerPush);
        assert!(!scenarios[0].needs_raw_seed);
        assert!(!scenarios[0].needs_docker_seed);
    }

    #[test]
    fn get_scenarios_docker_pull_needs_seed() {
        let scenarios = get_scenarios("docker-pull");
        assert_eq!(scenarios.len(), 1);
        assert!(scenarios[0].needs_docker_seed);
    }

    #[test]
    fn get_scenarios_docker_mixed() {
        let scenarios = get_scenarios("docker-mixed");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].ops.len(), 2);
        assert!(scenarios[0].needs_docker_seed);
    }

    #[test]
    fn get_scenarios_all_returns_six() {
        let scenarios = get_scenarios("all");
        assert_eq!(scenarios.len(), 6);
        let names: Vec<&str> = scenarios.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "raw-upload",
                "raw-download",
                "raw-mixed",
                "docker-push",
                "docker-pull",
                "docker-mixed"
            ]
        );
    }

    #[test]
    fn get_scenarios_unknown_returns_empty() {
        let scenarios = get_scenarios("nonexistent");
        assert!(scenarios.is_empty());
    }

    #[test]
    fn pick_op_single_op() {
        let ops = vec![WeightedOp {
            op_type: OpType::RawUpload,
            weight: 100,
        }];
        // Any rng_val should give the single op
        for v in 0..200 {
            assert_eq!(pick_op(&ops, v), OpType::RawUpload);
        }
    }

    #[test]
    fn pick_op_two_ops_50_50() {
        let ops = vec![
            WeightedOp {
                op_type: OpType::RawUpload,
                weight: 50,
            },
            WeightedOp {
                op_type: OpType::RawDownload,
                weight: 50,
            },
        ];
        // Values 0-49 mod 100 => RawUpload, 50-99 => RawDownload
        assert_eq!(pick_op(&ops, 0), OpType::RawUpload);
        assert_eq!(pick_op(&ops, 49), OpType::RawUpload);
        assert_eq!(pick_op(&ops, 50), OpType::RawDownload);
        assert_eq!(pick_op(&ops, 99), OpType::RawDownload);
    }

    #[test]
    fn pick_op_respects_weights() {
        let ops = vec![
            WeightedOp {
                op_type: OpType::RawDownload,
                weight: 60,
            },
            WeightedOp {
                op_type: OpType::RawUpload,
                weight: 30,
            },
            WeightedOp {
                op_type: OpType::RawDelete,
                weight: 10,
            },
        ];
        // Count distribution over full range
        let mut counts = [0u32; 3];
        for v in 0..100 {
            match pick_op(&ops, v) {
                OpType::RawDownload => counts[0] += 1,
                OpType::RawUpload => counts[1] += 1,
                OpType::RawDelete => counts[2] += 1,
                _ => panic!("unexpected op"),
            }
        }
        assert_eq!(counts[0], 60);
        assert_eq!(counts[1], 30);
        assert_eq!(counts[2], 10);
    }

    #[test]
    fn pick_op_wraps_large_values() {
        let ops = vec![WeightedOp {
            op_type: OpType::RawUpload,
            weight: 10,
        }];
        // Large values should still work via modulo
        assert_eq!(pick_op(&ops, 1_000_000), OpType::RawUpload);
    }
}
