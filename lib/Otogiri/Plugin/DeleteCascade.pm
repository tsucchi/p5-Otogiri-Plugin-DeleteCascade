package Otogiri::Plugin::DeleteCascade;
use 5.008005;
use strict;
use warnings;

our $VERSION = "0.01";

use Otogiri;
use Otogiri::Plugin;
use DBI;

our @EXPORT = qw(delete_cascade);

sub delete_cascade {
    my ($self, $table_name, $cond_href) = @_;
    my @child_table_names = _fetch_child_table_names($self, $table_name);
    my @parent_rows = $self->select($table_name, $cond_href);
    for my $child_table_name ( @child_table_names ) {
        _delete_child_tables($self, $table_name, $child_table_name, @parent_rows);
    }
    $self->delete($table_name, $cond_href);
}

sub _fetch_child_table_names {
    my ($db, $table_name) = @_;
    my $sql = <<"EOSQL";
SELECT DISTINCT table_constraints.table_name
   FROM information_schema.table_constraints
        JOIN information_schema.constraint_column_usage
          ON constraint_column_usage.table_catalog      = table_constraints.table_catalog
         AND constraint_column_usage.table_schema       = table_constraints.table_schema
         AND constraint_column_usage.constraint_catalog = table_constraints.constraint_catalog
         AND constraint_column_usage.constraint_name    = table_constraints.constraint_name
   WHERE table_constraints.constraint_type  ='FOREIGN KEY'
     AND constraint_column_usage.table_name = ?
 ;
EOSQL
    my @result = map { $_->{table_name} } $db->search_by_sql($sql, [$table_name]);
    return @result;
}

sub _delete_child_tables {
    my ($db, $parent_table_name, $child_table_name, @parent_rows) = @_;
    my @foreign_column_info = _fetch_foreign_column_info($db, $parent_table_name, $child_table_name);
    for my $foreign_column_info ( @foreign_column_info ) {
        for my $parent_row ( @parent_rows ) {
            my $child_delete_condition = {
                $foreign_column_info->{column_name} => $parent_row->{$foreign_column_info->{foreign_column_name}},
            };
            $db->delete_cascade($child_table_name, $child_delete_condition);
        }
    }
}

sub _fetch_foreign_column_info {
    my ($db, $parent_table_name, $child_table_name) = @_;
    my $sql = <<"EOSQL";
SELECT key_column_usage.column_name
     , constraint_column_usage.column_name AS foreign_column_name
  FROM information_schema.table_constraints
       JOIN information_schema.key_column_usage
         ON table_constraints.constraint_name = key_column_usage.constraint_name
       JOIN information_schema.constraint_column_usage
          ON constraint_column_usage.table_catalog      = table_constraints.table_catalog
         AND constraint_column_usage.table_schema       = table_constraints.table_schema
         AND constraint_column_usage.constraint_catalog = table_constraints.constraint_catalog
         AND constraint_column_usage.constraint_name    = table_constraints.constraint_name
  WHERE table_constraints.constraint_type   = 'FOREIGN KEY'
    AND table_constraints.table_name        = ?
    AND constraint_column_usage.table_name  = ?
;
EOSQL
    return $db->search_by_sql($sql, [$child_table_name, $parent_table_name]);
}



1;
__END__

=encoding utf-8

=head1 NAME

Otogiri::Plugin::DeleteCascade - It's new $module

=head1 SYNOPSIS

    use Otogiri::Plugin::DeleteCascade;

=head1 DESCRIPTION

Otogiri::Plugin::DeleteCascade is ...

=head1 LICENSE

Copyright (C) Takuya Tsuchida.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Takuya Tsuchida E<lt>takuya.tsuchida@gmail.comE<gt>

=cut

