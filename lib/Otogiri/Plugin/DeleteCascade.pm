package Otogiri::Plugin::DeleteCascade;
use 5.008005;
use strict;
use warnings;

our $VERSION = "0.01";

use Otogiri;
use Otogiri::Plugin;
use DBIx::Inspector;

our @EXPORT = qw(delete_cascade);

sub delete_cascade {
    my ($self, $table_name, $cond_href) = @_;
    $cond_href = $self->_deflate_param($table_name, $cond_href);
    my @child_table_info = _fetch_child_table_info($self, $table_name);
    my @parent_rows = $self->select($table_name, $cond_href);
    for my $child_table_info ( @child_table_info ) {
        _delete_child_tables($self, $child_table_info, @parent_rows);
    }
    $self->delete($table_name, $cond_href);
}

sub _fetch_child_table_info {
    my ($db, $table_name) = @_;
    my $inspector = DBIx::Inspector->new(dbh => $db->dbh);
    my $iter = $inspector->table($table_name)->pk_foreign_keys();
    my @result = ();
    while( my $fk = $iter->next ) {
        push @result, {
            fktable_name  => $fk->fktable_name,
            pkcolumn_name => $fk->pkcolumn_name,
            fkcolumn_name => $fk->fkcolumn_name,
        }
    }
    return @result;
}

sub _delete_child_tables {
    my ($db, $child_table_info, @parent_rows) = @_;
    for my $parent_row ( @parent_rows ) {
        my $child_table_name   = $child_table_info->{fktable_name};
        my $parent_column_name = $child_table_info->{pkcolumn_name};
        my $child_column_name  = $child_table_info->{fkcolumn_name};

        my $child_delete_condition = {
            $child_column_name => $parent_row->{$parent_column_name},
        };
        $db->delete_cascade($child_table_name, $child_delete_condition);
    }
}


1;
__END__

=encoding utf-8

=head1 NAME

Otogiri::Plugin::DeleteCascade - Otogiri Plugin for cascading delete by following FK columns

=head1 SYNOPSIS

    use Otogiri;
    use Otogiri::Plugin;

    Otogiri->load_plugin('DeleteCascade');

    my $db = Otogiri->new( connect_info => $connect_info );
    $db->insert('parent_table', { id => 123, value => 'aaa' });
    $db->insert('child_table',  { parent_id => 123, value => 'bbb'}); # child.parent_id referes parent_table.id(FK)

    $db->delete_cascade('parent_table', { id => 123 }); # both parent_table and child_table are deleted.

=head1 DESCRIPTION

Otogiri::Plugin::DeleteCascade is plugin for L<Otogiri> which provides cascading delete feature.
loading this plugin, C<delete_cascade> method is exported. C<delete_cascade> follows Foreign Keys(FK) and
delete data referred in these key.

=head1 METHOD

=head2 $self->delete_cascade($table_name, $cond_href);

Delete rows that matched to $cond_href and child table rows that can be followed by Foreign Keys.

=head1 LICENSE

Copyright (C) Takuya Tsuchida.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Takuya Tsuchida E<lt>tsucchi@cpan.orgE<gt>

=cut

