#!/usr/bin/perl

use strict;
use Text::Glob qw(match_glob);

open(my $batches_file, $ARGV[0]) or die $!;
my $batch_map = {};
my $found_batches = {};
while (<$batches_file>) {
    s/\r?\n//;
    my @fields = split(/\t/);
    $fields[0],"\n";
    $batch_map->{$fields[0]}=$fields[1];
}
open(my $files_file, "logging_cpp_files.txt") or die $!;
while (<$files_file>) {
    next if /^#/;
    s/\r?\n//;
    my $match = "";
    foreach my $key (sort keys %$batch_map) {
        my $regex;
        if ($key =~ /\*/) {
            $regex = $key;
            if ($key =~ /\//) {
                $regex =~ s[\*][\[^\/\]\*]g;
            } else {
                $regex =~ s[\*][\.\*]g;
            }
            $regex = qr($regex);
        } else {
            $regex = "$key/.*";
        }
        if (m[$regex]) {
            if (length($key)>length($match)) {
                $match = $key;
            }
        }
    }
    if ($match eq "") {
        #print("?\tunknown\t$_\n");
    } else {
        push(@{$found_batches->{$batch_map->{$match}}},$_);
        #print("$batch_map->{$match}\t$match\t$_\n");
    }
}

for my $batch (sort keys %$found_batches) {
    my @files = @{$found_batches->{$batch}};
    print(qw(git cifa),"\n");
    print(./logv1tologv2,@files,"\n"); 
    #python ~/git/kernel-tools/codereview/upload.py --git_no_find_copies -y -m "structured logging auto-conversion" HEAD
}
