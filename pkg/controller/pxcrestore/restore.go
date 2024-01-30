package pxcrestore

import (
	"context"

	"github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
)

func (r *ReconcilePerconaXtraDBClusterRestore) restore(ctx context.Context, cr *api.PerconaXtraDBClusterRestore, bcp *api.PerconaXtraDBClusterBackup, cluster *api.PerconaXtraDBCluster) error {
	if cluster.Spec.Backup == nil {
		return errors.New("undefined backup section in a cluster spec")
	}

	restorer, err := r.getRestorer(cr, bcp, cluster)
	if err != nil {
		return errors.Wrap(err, "failed to get restorer")
	}
	job, err := restorer.Job()
	if err != nil {
		return errors.Wrap(err, "failed to get restore job")
	}
	if err = controllerutil.SetControllerReference(cr, job, r.scheme); err != nil {
		return err
	}

	if err = restorer.Init(ctx); err != nil {
		return errors.Wrap(err, "failed to init restore")
	}

	err = r.client.Create(ctx, job)
	if err != nil {
		return errors.Wrap(err, "create job")
	}

	return nil
}

func (r *ReconcilePerconaXtraDBClusterRestore) pitr(ctx context.Context, cr *api.PerconaXtraDBClusterRestore, bcp *api.PerconaXtraDBClusterBackup, cluster *api.PerconaXtraDBCluster) error {
	restorer, err := r.getRestorer(cr, bcp, cluster)
	if err != nil {
		return errors.Wrap(err, "failed to get restorer")
	}
	job, err := restorer.PITRJob()
	if err != nil {
		return errors.Wrap(err, "failed to create pitr restore job")
	}
	if err := controllerutil.SetControllerReference(cr, job, r.scheme); err != nil {
		return err
	}
	if err = restorer.Init(ctx); err != nil {
		return errors.Wrap(err, "failed to init restore")
	}

	err = r.client.Create(ctx, job)
	if err != nil {
		return errors.Wrap(err, "create job")
	}

	return nil
}

func (r *ReconcilePerconaXtraDBClusterRestore) validate(ctx context.Context, cr *api.PerconaXtraDBClusterRestore, bcp *api.PerconaXtraDBClusterBackup, cluster *api.PerconaXtraDBCluster) error {
	restorer, err := r.getRestorer(cr, bcp, cluster)
	if err != nil {
		return errors.Wrap(err, "failed to get restorer")
	}
	job, err := restorer.Job()
	if err != nil {
		return errors.Wrap(err, "failed to create restore job")
	}
	if err := restorer.ValidateJob(ctx, job); err != nil {
		return errors.Wrap(err, "failed to validate job")
	}

	if cr.Spec.PITR != nil {
		job, err := restorer.PITRJob()
		if err != nil {
			return errors.Wrap(err, "failed to create pitr restore job")
		}
		if err := restorer.ValidateJob(ctx, job); err != nil {
			return errors.Wrap(err, "failed to validate job")
		}
	}
	if err := restorer.Validate(ctx); err != nil {
		return errors.Wrap(err, "failed to validate backup existence")
	}
	return nil
}
