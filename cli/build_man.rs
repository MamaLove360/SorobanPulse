use clap::CommandFactory;
use clap_complete::Shell;

fn main() {
    let out_dir = env!("OUT_DIR");
    let mut app = spulse::Cli::command();

    // Generate shell completions
    clap_complete::generate(Shell::Bash, &mut app.clone(), "spulse", std::path::Path::new(out_dir));
    clap_complete::generate(Shell::Zsh, &mut app.clone(), "spulse", std::path::Path::new(out_dir));
    clap_complete::generate(Shell::Fish, &mut app.clone(), "spulse", std::path::Path::new(out_dir));

    // Generate man page
    let man = clap_mangen::Man::new(app);
    let mut buffer = Vec::new();
    man.render(&mut buffer).expect("failed to render man page");

    let man_path = std::path::Path::new(out_dir).join("spulse.1");
    std::fs::write(&man_path, buffer).expect("failed to write man page");

    println!("cargo:warning=Man page generated at {}", man_path.display());
}
