//! Parse HandBrake preset JSON and extract the preset name to pass to `-Z`.
//!
//! Presets can be folder-structured (`Folder: true` + `ChildrenArray`), so we
//! recurse to find leaf presets rather than assuming `PresetList[0]`.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct PresetFile {
    #[serde(rename = "PresetList", default)]
    preset_list: Vec<PresetNode>,
}

#[derive(Debug, Deserialize)]
struct PresetNode {
    #[serde(rename = "PresetName")]
    preset_name: Option<String>,
    #[serde(rename = "FileFormat")]
    file_format: Option<String>,
    #[serde(rename = "Folder", default)]
    folder: bool,
    #[serde(rename = "ChildrenArray", default)]
    children: Vec<PresetNode>,
}

/// The selected preset's name and (optional) output container format.
#[derive(Debug, Clone)]
pub struct PresetInfo {
    pub name: String,
    pub file_format: Option<String>,
}

/// Load a preset file and pick a leaf preset.
///
/// If `override_name` is given, selects the matching leaf; otherwise the first.
pub fn load_preset(path: &Path, override_name: Option<&str>) -> Result<PresetInfo> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read preset file: {}", path.display()))?;
    let parsed: PresetFile = serde_json::from_str(&text)
        .with_context(|| format!("failed to parse preset JSON: {}", path.display()))?;
    let mut leaves = Vec::new();
    collect_leaves(&parsed.preset_list, &mut leaves);
    select(leaves, override_name, path)
}

fn collect_leaves<'a>(nodes: &'a [PresetNode], out: &mut Vec<&'a PresetNode>) {
    for node in nodes {
        if node.folder || !node.children.is_empty() {
            collect_leaves(&node.children, out);
        } else if node.preset_name.is_some() {
            out.push(node);
        }
    }
}

fn select(
    leaves: Vec<&PresetNode>,
    override_name: Option<&str>,
    path: &Path,
) -> Result<PresetInfo> {
    let chosen = match override_name {
        Some(name) => find_named(&leaves, name)
            .with_context(|| format!("preset '{}' not found in {}", name, path.display()))?,
        None => *leaves
            .first()
            .with_context(|| format!("no presets found in {}", path.display()))?,
    };
    to_info(chosen)
}

fn find_named<'a>(leaves: &[&'a PresetNode], name: &str) -> Option<&'a PresetNode> {
    leaves
        .iter()
        .find(|n| n.preset_name.as_deref() == Some(name))
        .copied()
}

fn to_info(node: &PresetNode) -> Result<PresetInfo> {
    match &node.preset_name {
        Some(name) => Ok(PresetInfo {
            name: name.clone(),
            file_format: node.file_format.clone(),
        }),
        None => bail!("preset leaf has no PresetName"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(name: &str, content: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("hbwatch-test-{name}.json"));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn reads_flat_preset() {
        let path = write_temp(
            "flat",
            r#"{"PresetList":[{"PresetName":"My 1080p","FileFormat":"av_mp4"}]}"#,
        );
        let info = load_preset(&path, None).unwrap();
        assert_eq!(info.name, "My 1080p");
        assert_eq!(info.file_format.as_deref(), Some("av_mp4"));
    }

    #[test]
    fn recurses_into_folder_structured_preset() {
        let path = write_temp(
            "nested",
            r#"{"PresetList":[{"Folder":true,"PresetName":"Group",
                "ChildrenArray":[{"PresetName":"Deep 4K","FileFormat":"av_mkv"}]}]}"#,
        );
        let info = load_preset(&path, None).unwrap();
        assert_eq!(info.name, "Deep 4K");
    }

    #[test]
    fn honors_name_override() {
        let path = write_temp(
            "override",
            r#"{"PresetList":[{"PresetName":"A"},{"PresetName":"B"}]}"#,
        );
        let info = load_preset(&path, Some("B")).unwrap();
        assert_eq!(info.name, "B");
    }

    #[test]
    fn errors_on_missing_override() {
        let path = write_temp("missing", r#"{"PresetList":[{"PresetName":"A"}]}"#);
        assert!(load_preset(&path, Some("Z")).is_err());
    }
}
